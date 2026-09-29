from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .groupBy("channel")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("stddev_discount") < 50)
)
