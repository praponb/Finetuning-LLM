from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5]))
    .groupBy("store_id")
    .agg(
        (F.max("units") - F.min("units")).alias("units_range"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("avg_revenue").desc(), F.col("store_id").desc())
)
