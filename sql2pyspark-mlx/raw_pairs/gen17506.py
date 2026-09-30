from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.hour("started_at").between(11, 15))
    .withColumn("country_filled", F.coalesce("country", F.lit("Unknown")))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

matched = (
    eligible
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest_match = (
    matched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page"))
)

remaining_keys = (
    latest_match
    .select("browser")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("browser")
    )
)

reconciled = (
    latest_match
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

totals = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
        F.when(F.col("subscription_id") >= 10, "PREMIUM").otherwise("STANDARD").alias("subscription_id_tier"),
    )
    .agg(
        F.min("monthly_fee").alias("min_monthly_fee"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sum(F.when(F.col("page") == "/search", F.col("monthly_fee"))).alias("search_total_monthly_fee"),
    )
    .filter(F.col("search_total_monthly_fee") > 0)
)

result = (
    totals
    .select(
        "activity_month",
        "subscription_id_tier",
        "min_monthly_fee",
        "monthly_fee_p90",
        "browser_list",
    )
)
