from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("delivered_date").alias("last_delivered_date"),
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("os.last_delivered_date"),
        F.col("os.kerry_count"),
    )
)
