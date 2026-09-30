from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w3 = Window.orderBy(F.col("monthly_fee").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w2))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w3))
)

enriched = (
    with_windows
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.monthly_fee",
        "a.monthly_fee_share",
        "a.monthly_fee_percent_rank",
        F.col("d.segment"),
        F.col("d.age"),
    )
)

base = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (F.col("segment").eqNullSafe(F.lit("SMB"))))
    .withColumn("monthly_fee_per_monthly_fee_share", (F.col("monthly_fee") / F.nullif("monthly_fee_share", F.lit(0))))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("OTHER")))
)

enriched_subscriptions = (
    base
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country") == "DE")
    .select("src.*", F.col("b.signup_date"), F.col("b.email"), F.col("b.credit_limit"))
)

valid = (
    enriched_subscriptions
    .filter(F.col("segment").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "*",
        F.last_day("signup_date").alias("signup_date_month_end"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

valid_subscriptions = (
    valid
    .filter((F.col("segment").eqNullSafe(F.lit("Corporate"))) & (F.col("monthly_fee_per_monthly_fee_share") > 71))
    .select(
        "*",
        F.month("signup_date_month_end").alias("signup_date_month_end_month"),
        (F.col("monthly_fee_share") - F.col("monthly_fee")).alias("monthly_fee_share_minus_monthly_fee"),
    )
)

matched = (
    valid_subscriptions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.signup_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

totals = (
    matched
    .groupBy(
        F.date_trunc("MONTH", "signup_date_month_end").alias("activity_month"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)

result = (
    totals
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "activity_month",
        "customer_id",
        "smb_events",
        "num_segments",
        F.col("sum_account_balance"),
    )
    .orderBy("smb_events")
)
