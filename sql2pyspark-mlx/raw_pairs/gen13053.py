from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.channel").alias("channel"), F.col("s.method").alias("method"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.amount")).alias("min_amount"),
    )
)

result = (
    dim_totals
    .select(
        "channel",
        "method",
        "min_amount",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "CORE").when(F.col("channel").rlike("^(web|partner)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("channel_zone"),
    )
    .orderBy(F.col("method").desc_nulls_first())
)
