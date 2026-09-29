from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("store_id").isin([1, 3, 4, 5]))

store_sales_stats = (
    src
    .groupBy(
        "store_id",
        F.when(F.col("units") >= 38, "high").when(F.col("units") >= 14, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)

result = (
    store_sales_stats
    .select("store_id", "units_bucket")
    .orderBy("store_id")
)
