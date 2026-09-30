from pyspark.sql import functions as F

by_dimension = (
    df_payments
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.channel").alias("channel"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.amount")).alias("avg_amount"),
    )
)

result = (
    by_dimension
    .select(
        "channel",
        "payment_status",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "PRIORITY").when(F.col("channel").rlike("^(store|partner)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
