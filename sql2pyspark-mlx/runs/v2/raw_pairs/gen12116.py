from pyspark.sql import functions as F

src = df_store_sales.filter((F.col("revenue") < 1100) & (F.col("revenue") > 3800))

store_sales_stats = (
    src
    .groupBy("product_id", "store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)

result = (
    store_sales_stats
    .orderBy("product_id", F.col("store_id").desc())
    .limit(10)
)
