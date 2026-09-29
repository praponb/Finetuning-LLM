from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") < "2025-06-30") & F.month("delivered_date").isin([1, 8, 10]) & (F.col("status") == "RETURNED"))
    .groupBy("status", "carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") >= 1)
    .drop("cost_count")
    .orderBy("carrier", F.col("status").desc())
)
