from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "IN_TRANSIT") & F.col("status").isin(["LOST", "IN_TRANSIT"]))
    .rollup("carrier", "status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
