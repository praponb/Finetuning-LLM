from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("channel", "payment_method").orderBy(F.col("days_since_order_date").desc(), "id")

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("status") == "CANCELLED"))
    .select(
        "*",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lag("shipping_fee").over(w_recent).alias("prev_shipping_fee"),
        F.lead("days_since_order_date").over(w_recent).alias("next_days_since_order_date"),
        F.lag("days_since_order_date").over(w_recent).alias("prev_days_since_order_date"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.days_since_order_date",
        "s.channel_clean",
        "s.prev_shipping_fee",
        "s.row_seq",
        F.col("c.status").alias("payment_status"),
        F.col("c.method").alias("payment_method"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.paid_at") >= "2025-02-01"), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isin([6, 28, 31, 40])), "left_semi")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "status",
        "amount",
        "days_since_order_date",
        "channel_clean",
        "row_seq",
    )
)

eligible_orders = (
    ranked
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isNotNull()), "left_anti")
)

result = (
    eligible_orders
    .groupBy(
        "status",
        F.when(F.col("row_seq") >= 9, "HIGH").otherwise("LOW").alias("row_seq_tier"),
    )
    .agg(
        F.percentile_approx("days_since_order_date", [0.25, 0.5, 0.75]).alias("days_since_order_date_quartiles"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)
