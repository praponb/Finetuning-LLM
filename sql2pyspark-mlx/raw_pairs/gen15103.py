from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 8]) & (F.col("store_id") == 2) & F.col("units").between(28, 35))
    .groupBy("product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.min("units").alias("min_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("avg_revenue").desc(), "product_id")
)
