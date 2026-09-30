from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id"), F.col("s.promo_id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("status", "plan").orderBy(F.col("order_date").desc())
w4 = Window.partitionBy("quarter_start").orderBy(F.col("shipping_fee_per_standard").desc())

with_window = (
    df_orders
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

not_flagged = (
    with_window
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isNotNull()), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

joined = (
    with_last
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.store_name"))
)

matched = (
    joined
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_last_orders = (
    matched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

deduped = (
    with_last_orders
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("quarter", "shipment_shipped_date").alias("quarter_start"),
        "promo_id",
        F.date_trunc("month", "shipment_shipped_date").alias("activity_month"),
    )
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("monthly_fee", 0.5).alias("monthly_fee_p50"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.sum("shipping_fee") / F.nullif(F.count(F.when(F.col("plan") == "standard", 1)), F.lit(0)), 2).alias("shipping_fee_per_standard"),
    )
    .withColumn("shipping_fee_per_standard_dense_rank", F.dense_rank().over(w4))
    .orderBy(F.col("shipping_fee_per_standard").desc_nulls_last())
)
