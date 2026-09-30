from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") > "2025-04-01") & (F.col("revenue") > 2800) & (F.col("units") < 23))
    .groupBy("store_id", "product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.min("sale_date").alias("min_sale_date"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)
