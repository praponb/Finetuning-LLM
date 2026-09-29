from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-09-14", "2025-04-01") | (F.col("units") < 31))
    .groupBy("store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
        F.count("*").alias("total_count"),
    )
)
