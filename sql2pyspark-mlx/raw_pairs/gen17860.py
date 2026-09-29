from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w2 = Window.partitionBy(F.month("paid_at"), "order_id")
w_recent = Window.partitionBy("order_id", "status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy(F.col("amount").desc_nulls_first())

kept_keys = (
    df_payments
    .select("id")
    .exceptAll(
        df_payments
        .filter(F.col("status") == "FAILED")
        .select("id")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("method") == "bank_transfer")
    .withColumn("amount_avg_90d", F.avg("amount").over(w))
)

filtered = (
    with_windows
    .filter(F.col("amount_avg_90d") == 1136)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "amount_avg_90d",
        F.date_format("paid_at", "yyyy-MM-dd").alias("paid_at_label"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

cleaned = (
    filtered
    .filter((F.col("paid_at") >= F.lit("2023-05-31").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
)

enriched = (
    cleaned
    .select(
        "id",
        "amount",
        "paid_at",
        "amount_avg_90d",
        "resolved_method",
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.avg("amount_avg_90d").over(w_recent).alias("amount_avg_90d_avg_7d"),
        F.cume_dist().over(w4).alias("amount_cume_dist"),
    )
)

result = (
    enriched
    .select(
        "id",
        "paid_at",
        "amount_avg_90d",
        "resolved_method",
        "amount_share",
        "amount_avg_90d_avg_7d",
        "amount_cume_dist",
    )
    .orderBy(F.col("paid_at").desc(), "amount_cume_dist")
)
