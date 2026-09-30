from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("weeks_since_order_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status", "channel").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id"), F.col("s.promo_id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "order_date") / 7).alias("weeks_since_order_date"),
    )
)

enriched = (
    base
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "shipping_fee",
        F.sum("amount").over(w).alias("last4_sum_amount"),
        F.max("amount").over(w).alias("last4_max_amount"),
        F.count("amount").over(w).alias("last4_count_amount"),
        F.avg("amount").over(w).alias("last4_avg_amount"),
        F.last("shipping_fee", ignorenulls=True).over(w2).alias("last_known_shipping_fee"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_anti")
)

matched = (
    kept
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_latest = (
    matched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.order_date",
        "s.amount",
        "s.last4_avg_amount",
        "s.last_known_shipping_fee",
        "s.promo_id",
        "s.promo_code",
        "s.discount_pct",
        F.col("c.unit_price"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("unit_price") >= 380, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
        F.date_trunc("WEEK", "order_date").alias("week_start"),
        F.date_trunc("QUARTER", "order_date").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.percentile_approx("last4_avg_amount", [0.25, 0.5, 0.75]).alias("last4_avg_amount_quartiles"),
        F.round(F.avg("last_known_shipping_fee"), 2).alias("avg_last_known_shipping_fee"),
    )
)
