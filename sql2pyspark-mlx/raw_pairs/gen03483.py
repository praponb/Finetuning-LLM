from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 25) | F.col("sale_date").between("2023-06-23", "2025-08-01"))
)
