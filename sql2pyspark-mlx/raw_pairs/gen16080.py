from pyspark.sql import functions as F

with_dims = (
    df_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(~F.col("d.country").isin(["TH", "UK", "DE"]))
    .select("a.*", F.col("d.age"), F.col("d.segment"), F.col("d.signup_date"))
)

matched_keys = (
    with_dims
    .select("id", "browser")
    .intersect(
        df_sessions
        .filter(F.col("country") == "DE")
        .select("id", "browser")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec").isNotNull()), "left_anti")
)

with_dims_sessions = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.country").alias("customer_country"))
)

kept = (
    with_dims_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.signup_date")), "left_anti")
)

not_flagged_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") != 205), "left_anti")
)

with_dims_sessions_2 = (
    not_flagged_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.segment").alias("customer_segment"))
)

summary = (
    with_dims_sessions_2
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.avg("age").alias("avg_age"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("customer_country") == "SG", 1)), F.lit(0)), 2).alias("age_per_sg"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.max("age").alias("max_age"),
    )
    .filter(F.col("avg_age") >= 0)
)

result = (
    summary
    .select("quarter_start", "avg_age", "age_per_sg", "browser_list")
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
