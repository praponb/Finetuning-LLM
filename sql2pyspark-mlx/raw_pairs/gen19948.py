from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id", "method").orderBy("amount", "id")

scored = (
    df_payments
    .filter(F.col("method") != "card")
    .select(
        "*",
        F.lead("amount").over(w_ordered).alias("next_amount"),
        F.lag("amount").over(w_ordered).alias("prev_amount"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.amount").alias("order_amount"),
        F.col("p.channel"),
        F.col("p.shipping_fee"),
    )
)

enriched_payments = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.paid_at",
        "s.next_amount",
        "s.prev_amount",
        "s.order_amount",
        "s.channel",
        F.col("p.shipping_fee").alias("order_shipping_fee"),
        F.col("p.discount"),
        F.col("p.status").alias("order_status"),
    )
)

cleaned = (
    enriched_payments
    .filter(F.col("method").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .select("*", F.round((F.col("discount") * 100), 2).alias("discount_scaled"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.prev_amount",
        "s.channel",
        "s.order_shipping_fee",
        "s.discount",
        "s.discount_scaled",
        F.col("p.channel").alias("order_channel"),
        F.col("p.amount"),
    )
)

valid = (
    with_dims
    .filter(F.col("order_id").isNotNull())
    .select(
        "*",
        F.coalesce("order_shipping_fee", F.lit(0)).alias("order_shipping_fee_filled"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("order_shipping_fee_filled") >= 85, "HIGH").otherwise("LOW").alias("order_shipping_fee_filled_tier"),
        F.when(F.col("order_shipping_fee") >= 41, "HIGH").otherwise("LOW").alias("order_shipping_fee_tier"),
    )
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("prev_amount", 0.95).alias("prev_amount_p95"),
        F.avg("discount_scaled").alias("avg_discount_scaled"),
    )
    .orderBy(F.col("prev_amount_p95").desc_nulls_first())
)
