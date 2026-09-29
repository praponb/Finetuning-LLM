from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2024-07-01") & (F.col("revenue").between(1700, 2000) | (F.col("units") <= 37)))
    .select(
        "id",
        "revenue",
        "sale_date",
        F.dayofweek("sale_date").alias("sale_date_dayofweek"),
    )
)
