from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("order_id").isNull() | (F.col("status") == "DELIVERED"))

status_summary = (
    valid_shipments
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)

result = (
    status_summary
    .filter(F.col("stddev_cost") > 45)
    .select("status", "status_list", "stddev_cost")
)
