from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("avg_revenue") >= 42)
)
