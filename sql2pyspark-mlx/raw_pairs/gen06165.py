from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("end_date", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("plan").orderBy(F.col("subscription_monthly_fee_share").asc_nulls_first())
w4 = Window.partitionBy(F.month("start_date")).orderBy("subscription_monthly_fee_share_percent_rank")
w5 = Window.partitionBy("status", "plan").orderBy(F.col("last4_avg_subscription_monthly_fee").desc())
w6 = Window.partitionBy("status", "plan")

matched = (
    df_subscriptions
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date") >= F.col("sub.start_date")) & (F.col("src.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

joined = (
    matched
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "left")
    .select("a.*", F.col("d.country"), F.col("d.city"))
)

scored = (
    joined
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "subscription_id",
        "subscription_monthly_fee",
        "country",
        F.min("subscription_monthly_fee").over(w_running).alias("last4_min_subscription_monthly_fee"),
        F.avg("subscription_monthly_fee").over(w_running).alias("last4_avg_subscription_monthly_fee"),
        F.count("subscription_monthly_fee").over(w_running).alias("last4_count_subscription_monthly_fee"),
        F.round((F.col("subscription_monthly_fee") / F.sum("subscription_monthly_fee").over(w2)), 4).alias("subscription_monthly_fee_share"),
    )
)

remaining_keys = (
    scored
    .select("plan")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date").between("2023-11-01", "2024-02-18"))
        .select("plan")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "plan",
        "start_date",
        "status",
        "last4_min_subscription_monthly_fee",
        "last4_avg_subscription_monthly_fee",
        "subscription_monthly_fee_share",
        F.percent_rank().over(w3).alias("subscription_monthly_fee_share_percent_rank"),
    )
)

enriched_subscriptions = (
    enriched
    .select(
        "*",
        F.rank().over(w4).alias("subscription_monthly_fee_share_percent_rank_rank"),
        F.percent_rank().over(w5).alias("last4_avg_subscription_monthly_fee_percent_rank"),
        F.count("subscription_monthly_fee_share").over(w6).alias("status_count_subscription_monthly_fee_share"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "plan",
        "last4_min_subscription_monthly_fee",
        "last4_avg_subscription_monthly_fee",
        "subscription_monthly_fee_share",
        "subscription_monthly_fee_share_percent_rank",
        "subscription_monthly_fee_share_percent_rank_rank",
        "last4_avg_subscription_monthly_fee_percent_rank",
    )
)
