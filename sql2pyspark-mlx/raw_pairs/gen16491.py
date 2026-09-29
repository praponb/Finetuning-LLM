from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2025-07-19") & F.col("units").between(23, 38))
)
