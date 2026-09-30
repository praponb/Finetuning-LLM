from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-10-05").cast("date")) & F.col("plan").isin(["enterprise", "basic"]))
    .withColumn("months_end_date_to_start_date", F.round((F.datediff("start_date", "end_date") / 30), 1))
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
)

base_subscriptions = (
    base
    .filter((F.col("end_date") > F.lit("2023-04-14").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
    )
)

with_window = (
    base_subscriptions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.end_date") >= F.col("sub.start_date")) & (F.col("s.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

matched_keys = (
    with_window
    .select("plan", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("plan") == "premium")
        .select("plan", "id")
    )
)

kept_rows = (
    with_window
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.start_date",
        "s.start_date_dayofweek",
        "s.subscription_id",
        "s.subscription_plan",
        F.col("b.signup_date"),
    )
)

joined_subscriptions = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.name").like("Oli%"))
    .select(
        "e.id",
        "e.customer_id",
        "e.start_date_dayofweek",
        F.col("b.email"),
        F.col("b.age"),
    )
)

filtered = (
    joined_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
        F.length("email").alias("email_length"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("start_date_dayofweek", 0.5).alias("start_date_dayofweek_p50"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("start_date_dayofweek_p50") < 1000000)
    .orderBy(F.col("customer_id").desc())
)
