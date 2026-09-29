from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "RETURNED", "LOST"]))
    .groupBy("order_id")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("os.last_shipped_date"),
    )
    .orderBy(F.col("id").desc(), "customer_id")
)
