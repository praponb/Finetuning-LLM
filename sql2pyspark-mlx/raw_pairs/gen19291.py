from pyspark.sql import functions as F

matched = (
    df_subscriptions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date") >= F.col("sub.start_date")) & (F.col("a.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_window = (
    matched
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

enriched = (
    with_window
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.name"))
)

base = (
    enriched
    .filter(F.col("subscription_id") <= 8)
    .withColumn("adjusted_subscription_id_2", F.round((F.col("subscription_id_2") * 0.9), 2))
    .withColumn("subscription_monthly_fee_2_int", F.col("subscription_monthly_fee_2").cast("int"))
    .withColumn("subscription_plan_clean", F.coalesce("subscription_plan", F.lit("OTHER")))
)

remaining_keys = (
    base
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee") >= 60)
        .select("plan")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "plan",
        F.col("subscription_monthly_fee_2").alias("SUBSCRIPTION_MONTHLY_FEE_2"),
        "adjusted_subscription_id_2",
        "subscription_monthly_fee",
    )
    .unpivot(
        ids=["plan"],
        values=["SUBSCRIPTION_MONTHLY_FEE_2", "adjusted_subscription_id_2", "subscription_monthly_fee"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
