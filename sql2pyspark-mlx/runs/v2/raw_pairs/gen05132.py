from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.min_cost") > 76)
    .select(
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("os.delivered_count"),
        F.col("os.min_cost"),
    )
    .orderBy(F.col("delivered_count").desc())
)
