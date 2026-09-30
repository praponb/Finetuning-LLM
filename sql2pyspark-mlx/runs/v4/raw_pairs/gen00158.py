from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("promo_code", "channel").orderBy("order_date", "id", "promo_id")
w3 = Window.partitionBy("store_id").orderBy("discount", "id", "promo_id")

base = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-06-14").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)

valid = (
    base
    .filter(~F.col("channel").isin(["store", "partner"]))
    .withColumn("order_date_period", F.date_trunc("month", "order_date"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") < 220), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("payment_amount"),
        F.col("c.status").alias("payment_status"),
    )
)

with_window = (
    latest_match
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics = (
    with_window
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w3))
)

attributed = (
    metrics
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.order_date_period") >= F.col("sub.start_date")) & (F.col("a.order_date_period") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.34, "PREMIUM").otherwise("STANDARD"))
    .withColumn("subscription_id_tier", F.when(F.col("subscription_id") >= 10, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["discount_pct_tier", "subscription_id_tier", "channel"], ["discount_pct_tier", "subscription_id_tier"], ["discount_pct_tier"], []],
        "discount_pct_tier",
        "subscription_id_tier",
        "channel",
    )
    .agg(
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)
