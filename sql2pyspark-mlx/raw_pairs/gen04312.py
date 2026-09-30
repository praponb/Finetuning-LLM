from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

cleaned = (
    df_sessions
    .filter((~F.col("country").eqNullSafe(F.lit("SG"))) & F.col("country").isNotNull())
    .withColumn("browser_lower", F.lower("browser"))
    .withColumn("clean_country", F.trim("country"))
)

filtered = (
    cleaned
    .filter(F.col("started_at") >= F.to_date(F.lit("2023-06-16")))
    .select(
        "*",
        F.trim("country").alias("clean_country_2"),
        F.concat_ws("-", "browser", "country").alias("browser_country_key"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

matched_keys = (
    filtered
    .select("customer_id")
    .intersect(
        df_sessions
        .filter(F.hour("started_at").between(11, 18))
        .select("customer_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_semi")
)

result = (
    eligible
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        "browser_lower",
        "clean_country",
        "browser_filled",
        F.col("avg_subscription_monthly_fee"),
    )
)
