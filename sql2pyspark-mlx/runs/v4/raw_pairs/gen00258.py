from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("os.first_shipped_date"),
    )
)
