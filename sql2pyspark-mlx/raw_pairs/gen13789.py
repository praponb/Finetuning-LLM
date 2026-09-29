from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-04-11").cast("date"))
    .select(
        "*",
        F.when(F.col("status") == "PENDING", 1).otherwise(0).alias("is_pending"),
    )
)

matched = (
    valid
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.order_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_window = (
    matched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    with_window
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"), F.col("b.store_name"))
)

base = (
    enriched
    .filter((F.col("order_date") > F.to_date(F.lit("2023-08-17"))) & (~F.col("city").eqNullSafe(F.lit("London"))))
    .select(
        "*",
        F.trim("channel").alias("clean_channel"),
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

result = (
    base
    .groupBy(
        "channel",
        F.when(F.col("promo_id") >= 9, "PREMIUM").otherwise("STANDARD").alias("promo_id_tier"),
        "subscription_id",
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_rows"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .orderBy(F.col("num_cities").desc_nulls_last())
)
