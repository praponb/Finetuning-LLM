from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .groupBy("order_id")
    .agg(F.max("shipped_date").alias("last_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("t.last_shipped_date"),
    )
)
