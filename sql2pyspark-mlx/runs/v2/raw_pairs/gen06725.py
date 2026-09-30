from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.max("units").alias("max_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)
