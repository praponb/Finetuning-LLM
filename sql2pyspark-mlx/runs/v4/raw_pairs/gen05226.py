from pyspark.sql import functions as F

with_window = (
    df_accounts
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .cube(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sum(F.when(F.col("plan") == "premium", F.col("balance")).otherwise(0)).alias("premium_balance"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
    )
)
