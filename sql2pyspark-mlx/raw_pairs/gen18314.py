from pyspark.sql import functions as F

with_window = (
    df_orders
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.order_date") >= F.col("pr.starts_on")) & (F.col("e.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

with_window_orders = (
    with_window
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("src.order_date") >= F.col("pr.starts_on")) & (F.col("src.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

eligible = (
    with_window_orders
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.shipped_date") < F.col("s.order_date")), "left_anti")
)

matched = (
    eligible
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.order_date") >= F.col("sub.start_date")) & (F.col("e.order_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

stats = (
    matched
    .cube("plan", F.date_trunc("YEAR", "order_date").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("promotion_promo_code")).alias("promotion_promo_code_list"),
        F.sum(F.when(F.col("plan") == "premium", F.col("promo_id_2"))).alias("premium_total_promo_id_2"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_rows"),
    )
)

result = (
    stats
    .select(
        "plan",
        "year_start",
        "promotion_promo_code_list",
        "completed_rows",
        F.when(F.col("plan").rlike("^(basic|premium)$"), "GROUP_B").when(F.col("plan").rlike("^(standard|enterprise)$"), "CORE").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("completed_rows").desc_nulls_last())
)
