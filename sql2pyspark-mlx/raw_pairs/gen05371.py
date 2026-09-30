from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("channel").isNotNull() & (F.col("order_date") > F.to_date(F.lit("2023-03-23"))))
    .select(
        "*",
        F.lower("channel").alias("channel_lower"),
        F.ceil("shipping_fee").alias("shipping_fee_whole"),
    )
)

matched = (
    cleaned
    .alias("s")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("s.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("s.order_date") >= F.col("pr.starts_on")) & (F.col("s.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

eligible = (
    matched
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_anti")
)

matched_orders = (
    eligible
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

summary = (
    matched_orders
    .groupBy(F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.count_if(F.col("promotion_promo_code") == "SPRING10").alias("spring10_events"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
    )
)

result = (
    summary
    .select("year_start", "spring10_events", "status_list")
    .orderBy(F.col("year_start").desc(), F.col("spring10_events").desc())
)
