from pyspark.sql import functions as F

joined = (
    df_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.channel"), F.col("o.order_date"), F.col("o.discount"))
)

result = (
    joined
    .select(
        "id",
        "method",
        "amount",
        "channel",
        "discount",
        F.when(F.col("channel").rlike("^(partner|store)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(web|mobile)$"), "EXTENDED").otherwise("GROUP_B").alias("channel_zone"),
    )
)
