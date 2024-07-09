from snowflake.snowpark import Session
import time

from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType, DateType

# Replace the below connection_parameters with your respective Snowflake account details
connection_parameters = {"account":"uv88689.ap-south-1.aws",
"user":"Yesubabu",
"password": "Yesu@123",
"role":"ACCOUNTADMIN",
"warehouse":"SANDY",
"database":"DIFFERENT_TABLES",
"schema":"LOVE"
}

# Create a Snowflake session
test_session = Session.builder.configs(connection_parameters).create()

# Use a warehouse
test_session.sql("use warehouse COMPUTE_WH").collect()

# Example dataframe creation and operations
# Example 1: Creating a dataframe with a set and schema
test = test_session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"])
test.show()

# Example 2: Creating a dataframe with inferred schema
test = test_session.create_dataframe([[1, 2, 3, "123"], [1, 2, 3, "ABC"], [1, 2, 3, "HPC"], [1, 2, 3, "EMD"]], schema=["a", "b", "c", "d"])
test.show()

# Example 3: Creating a dataframe with date values
test = test_session.create_dataframe([[1, 2, 3, '26-01-2022'], [1, 2, 3, '26-01-2022'], [1, 2, 3, '26-01-2022'], [1, 2, 3, '26-01-2022']], schema=["a", "b", "c", "d"])
test.show()

# Example 4: Creating a dataframe with float values
test = test_session.create_dataframe([[1, 2, 3, 26.897], [1, 2, 3, 27.897], [1, 2, 3, 29.897], [1, 2, 3, 39.897]], schema=["a", "b", "c", "d"])
test.show()

# Example 5: Creating a dataframe with null values
test = test_session.create_dataframe([[1, 2, 3, None], [1, 2, 3, None], [1, 2, 3, None], [1, 2, 3, None]], schema=["a", "b", "c", "d"])
test.show()

# Example 6: Creating a dataframe with nested structures (dict)
test = test_session.create_dataframe([[1, 2, 3, {"a": "hi"}], [1, 2, 3, None], [1, 2, 3, {"a": "Bye"}], [1, 2, 3, {"a": "hello"}]], schema=["a", "b", "c", "d"])
test.show()

# Example 7: Creating a dataframe with nested structures (list)
test = test_session.create_dataframe([[1, 2, 3, ["Hi"]], [1, 2, 3, None], [1, 2, 3, ["Hello"]], [1, 2, 3, ["Namaste"]]], schema=["a", "b", "c", "d"])

# Cache the dataframe for improved performance
test1 = test.cache_result()
test1.show()

# Check performance of show() for both dataframes
begin = time.time()
test.show()
end = time.time()
print(f"Total runtime of test.show() is {end - begin}")

begin = time.time()
test1.show()
end = time.time()
print(f"Total runtime of test1.show() (cached) is {end - begin}")
