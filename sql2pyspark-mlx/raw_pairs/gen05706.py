from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

cleaned = (
    exploded
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (~F.col("tag").eqNullSafe(F.lit("VIP"))))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.is_active"))
    .select("e.id", "e.customer_id", "e.tag", F.col("d.segment"), F.col("d.name"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.signup_date"))
)

grouped = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .cube("quarter_start", "customer_id")
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count_if(F.col("segment") == "Corporate").alias("corporate_events"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    grouped
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "quarter_start",
        "customer_id",
        "segment_list",
        "corporate_events",
        "num_tags",
        "gid",
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(
        F.col("min_subscription_monthly_fee").desc_nulls_last(),
        F.col("gid").desc(),
    )
)
