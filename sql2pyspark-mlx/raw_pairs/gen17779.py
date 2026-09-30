from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").asc_nulls_last(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("promptpay"))))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "method", "paid_at", "method_clean")
)

matched_keys = (
    latest
    .select("id")
    .exceptAll(
        df_payments
        .filter(F.col("method") == "cash")
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") <= 790)
    .select("e.order_id", "e.method", F.col("o.channel"), F.col("o.amount"))
)

cleaned = (
    enriched
    .filter(~F.col("method").isin(["bank_transfer", "cash"]))
    .select("*", F.concat("method", F.lit("/"), "channel").alias("method_channel"))
)

result = (
    cleaned
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "channel",
        "amount",
        F.when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_A").otherwise("PRIORITY").alias("channel_zone"),
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("channel").desc(), "order_id")
)
