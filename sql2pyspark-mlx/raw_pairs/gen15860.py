from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .groupBy("order_id")
    .agg(F.min("delivered_date").alias("first_delivered_date"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("os.first_delivered_date"),
    )
    .orderBy(F.col("discount").desc())
)
