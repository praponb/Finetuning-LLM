from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["LOST", "RETURNED"]))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .orderBy("stddev_cost", "status")
)
