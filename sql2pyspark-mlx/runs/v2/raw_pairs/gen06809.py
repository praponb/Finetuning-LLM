from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("avg_units") < 26)
    .select("store_id", "avg_units", "stddev_revenue")
    .orderBy("store_id")
)
