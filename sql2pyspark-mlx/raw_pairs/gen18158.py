from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy(F.year("end_date"), "plan").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-3, Window.currentRow)

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .filter(~F.col("d.name").like("%Davi%"))
    .select(
        "src.id",
        "src.customer_id",
        "src.end_date",
        "src.monthly_fee",
        F.col("d.city"),
    )
)

matched = (
    joined
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

metrics = (
    matched
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("city_avg_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.avg("subscription_monthly_fee").over(w3).alias("subscription_monthly_fee_avg_3"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.city",
        "s.subscription_id",
        "s.plan",
        "s.subscription_monthly_fee",
        "s.monthly_fee_share",
        "s.subscription_monthly_fee_avg_3",
        F.col("b.email"),
        F.col("b.name"),
        F.col("b.segment"),
    )
)

remaining_keys = (
    with_dims
    .select("id", "customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee") <= 240)
        .select("id", "customer_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .withColumn("monthly_fee_share_tier", F.when(F.col("monthly_fee_share") >= 0.67, "PREMIUM").otherwise("STANDARD"))
    .cube("plan", "monthly_fee_share_tier")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.avg(F.when(F.col("city") == "Chiang Mai", F.col("subscription_monthly_fee"))).alias("chiang_mai_avg_subscription_monthly_fee"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("plan")).alias("plan_list_2"),
        F.round(F.sum("subscription_monthly_fee_avg_3") / F.nullif(F.count(F.when(F.col("segment") == "Consumer", 1)), F.lit(0)), 2).alias("subscription_monthly_fee_avg_3_per_consumer"),
        F.grouping("plan").alias("gid"),
    )
)

result = (
    stats
    .select(
        "plan",
        "monthly_fee_share_tier",
        "singapore_rows",
        "num_segments",
        "chiang_mai_avg_subscription_monthly_fee",
        "plan_list_2",
        "gid",
        F.when(F.col("plan").rlike("^(enterprise|basic)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("monthly_fee_share_tier").desc_nulls_first())
)
