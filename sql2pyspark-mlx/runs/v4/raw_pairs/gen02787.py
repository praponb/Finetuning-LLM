from pyspark.sql import functions as F

matched_keys = (
    df_sessions
    .select("id")
    .intersectAll(
        df_sessions
        .filter(F.col("country") == "US")
        .select("id")
    )
)

reconciled = (
    df_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

enriched = (
    kept
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.age"), F.col("d.segment"), F.col("d.city"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.country").alias("customer_country"),
        F.col("p.name"),
        F.col("p.age").alias("customer_age"),
    )
)

summary = (
    with_dims
    .cube(
        F.when(F.col("age") >= 65, "HIGH").otherwise("LOW").alias("age_tier"),
        "browser",
        F.date_trunc("week", "started_at").alias("week_start"),
    )
    .agg(
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.count_if(F.col("country") == "US").alias("us_events"),
    )
)

result = (
    summary
    .select("age_tier", "week_start", "us_events")
    .orderBy("week_start", F.col("age_tier").desc())
)
