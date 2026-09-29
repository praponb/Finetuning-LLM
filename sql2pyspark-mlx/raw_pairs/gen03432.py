from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(~F.col("carrier").isin(["FedEx", "Kerry"]))
    .groupBy("order_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.stddev_cost") >= 78)
    .select(F.col("o.id"), F.col("o.amount"), F.col("os.stddev_cost"))
    .orderBy(F.col("stddev_cost").desc())
)
