# Data ingestion with dlt

## Data ingestion is the process of extracting data from a source, transporting it to a suitable environment, and preparing it for use. This often includes **normalizing, cleaning, and adding metadata**

---

## dlt is an open-source Python library that loads data from various, often messy data sources into well-structured, live datasets. It offers a lightweight interface for extracting data from REST APIs, SQL databases, cloud storage, Python data structures, and many more

## This project using dlt to extract data via from website via RESTapi and load data into local Duckdb

---

## insatll dlt in vertural environment and using jupyter notebook for programming

```bash
pip install -U dlt
pip install notebook, pandas
```

---

## extract data from website api endpoint pagination

```python
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


def paginated_getter():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        # Define pagination strategy - page-based pagination
        paginator=PageNumberPaginator(   # <--- Pages are numbered (1, 2, 3, ...)
            base_page=1,   # <--- Start from page 1
            total_path=None    # <--- No total count of pages provided by API, pagination should stop when a page contains no result items
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # remember about memory management and yield data


for page_data in paginated_getter():
    print(page_data)
    break
```

---

## Normalizing data

**Metadata tasks in data cleaning:**  

✅ **Add types** – Convert strings to numbers, timestamps, etc.  
✅ **Rename columns** – Ensure names follow a standard format (e.g., no special characters).  
✅ **Flatten nested dictionaries** – Bring values from nested dictionaries into the top-level row.  
✅ **Unnest lists/arrays** – Convert lists into **child tables** since they can’t be stored directly in a flat format.  

### Why use dlt for Normalizing data?

✅ **Automatically detects schema** – No need to define column types manually.  
✅ **Flattens nested JSON** – Converts complex structures into table-ready formats.  
✅ **Handles data type conversion** – Converts dates, numbers, and booleans correctly.  
✅ **Splits lists into child tables** – Ensures relational integrity for better analysis.  
✅ **Schema evolution support** – Adapts to changes in data structure over time.  

```python
# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name='taxi_data',
    destination='duckdb', # <--- to test pipeline locally
    dataset_name='taxi_rides',
)


# run the pipeline with the new resource
load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)


# explore loaded data
df = pipeline.dataset(dataset_type="default").rides.df()
```

---

## ****Alghough dlt is able to detect the first batch extracted data and build database schema automatically, if the newer extracted data contain different data type (ex: "age":19 vs. "age": "25"), dlt might change data type of the table and cause issue for query. It is good to set table schema manually**

```python
schema = dlt.Schema({
    "ride_id": "string",
    "pickup_time": "timestamp",
    "fare_amount": "float",
    "passenger_count": "integer"
})
pipeline = dlt.pipeline(destination="duckdb")
pipeline.run(data, table_name="rides", schema=schema, write_disposition="replace")
```

---

## dlt decorator **@dlt.resource and @dlt.source** are define the source of data

### 🔹 When to use @dlt.resource?

✅ When you want to **define an independent data resource** (usually a table), suitable for:
    Extract data directly from APIs, files or repositories
    The data needs to be transformed (e.g. yield to handle API paging)
🚀 Usage scenarios
    Pull data directly from the API
    Reading CSV / JSON files
    Read a single table from a source

```python
    # define an api resource 
    @dlt.resource(name="rides")  # "rides" is table name 
    def ny_taxi():
        client = RESTClient(base_url="https://api.example.com")

        # iterate API response
        for page in client.paginate("/taxi_rides"):
            yield page   
```

### 🔹 When to use @dlt.source?

✅ When you have **multiple related data sources and want to combine them into one source**, suitable for:
    Need to extract multiple different tables from the same source
    Need to manage multiple @dlt.resource in one function
    Some sources require shared parameters (such as API keys, authentication information)
🚀 Usage scenarios:
    Get a variety of data from the API
    Read multiple tables in a database
    Read multiple files of different types from an S3 bucket

```python
    # define a source, which including many data resoure(end points)
    @dlt.source(name="ny_taxi_data")
    def ny_taxi_source():
        client = RESTClient(base_url="https://api.example.com")

        @dlt.resource(name="rides")  # first resource
        def rides():
            for page in client.paginate("/taxi_rides"):
                yield page

        @dlt.resource(name="drivers")  # second resource
        def drivers():
            for page in client.paginate("/taxi_drivers"):
                yield page

        return rides, drivers
```

---

## Query data

```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            MIN(trip_dropoff_date_time)
            FROM rides;
            """
        )
    print(res)
```
