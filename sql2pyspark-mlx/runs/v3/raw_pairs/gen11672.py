from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 19) & F.col("units").isNotNull() & F.col("units").between(15, 26))
    .groupBy("store_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)
