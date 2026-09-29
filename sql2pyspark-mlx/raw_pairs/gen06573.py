from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("amount", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "promo_code").orderBy("discount_pct", "id", "promo_id")
w3 = Window.partitionBy("channel").orderBy("amount_times_discount", "id", "promo_id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("discount_pct", "id", "promo_id", "subscription_id")
w5 = Window.partitionBy(F.month("order_date"), "status")

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & F.col("store_id").isin([20, 34, 36]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
    )
)

matched = (
    cleaned
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.order_date") >= F.col("pr.starts_on")) & (F.col("a.order_date") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

enriched = (
    matched
    .withColumn("running_count_discount_pct", F.count("discount_pct").over(w_part))
    .withColumn("running_avg_discount_pct", F.avg("discount_pct").over(w_part))
    .withColumn("running_min_discount_pct", F.min("discount_pct").over(w_part))
    .withColumn("next_shipping_fee_int", F.lead("shipping_fee_int", 1).over(w2))
)

matched_orders = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.order_date") >= F.col("sub.start_date")) & (F.col("src.order_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

cleaned_orders = (
    matched_orders
    .filter((F.col("order_date") >= F.lit("2023-07-17").cast("date")) & (F.col("status") == "COMPLETED"))
    .select(
        "*",
        F.when(F.col("amount_times_discount") >= 88, "High").when(F.col("amount_times_discount") >= 30, "Medium").otherwise("Low").alias("amount_times_discount_tier"),
        F.coalesce("promo_code", F.lit("DIRECT")).alias("promo_code_filled"),
    )
)

scored = (
    cleaned_orders
    .select(
        "*",
        F.sum("shipping_fee_int").over(w3).alias("cumulative_shipping_fee_int"),
        F.lead("amount", 1).over(w_ordered).alias("next_amount"),
        F.round((F.col("subscription_id") / F.sum("subscription_id").over(w5)), 4).alias("subscription_id_share"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("amount_times_discount"))).alias("completed_total_amount_times_discount"),
        F.round(F.sum("running_avg_discount_pct") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("running_avg_discount_pct_per_premium"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("next_amount")).otherwise(0)).alias("enterprise_next_amount"),
        F.max(F.when(F.col("channel") == "mobile", F.col("cumulative_shipping_fee_int"))).alias("mobile_max_cumulative_shipping_fee_int"),
    )
    .filter(F.col("enterprise_rows") > 1)
)
