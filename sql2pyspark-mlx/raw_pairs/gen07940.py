from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("browser").orderBy("started_at", "id")
w5 = Window.orderBy("event_time", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("browser").orderBy("years_since_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_sessions
    .select(
        "id",
        "browser",
        "started_at",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.first("country", ignorenulls=True).over(w_hist).alias("first_known_country"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("started_at") > F.to_date(F.lit("2023-07-18"))) & F.col("browser").isNotNull())
    .withColumn("years_since_started_at", (F.datediff(F.current_date(), "started_at") / 365.25))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("OTHER")))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.started_at",
        "s.last_known_country",
        "s.first_known_country",
        "s.years_since_started_at",
        "s.browser_clean",
        F.col("c.event_time"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

with_gaps = (
    latest_match
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w))

enriched = (
    sessions_sessions
    .select(
        "id",
        "browser",
        "years_since_started_at",
        "browser_clean",
        "web_event_duration_sec",
        "session_no",
        F.avg("web_event_duration_sec").over(w5).alias("web_event_duration_sec_avg_3_prior"),
        F.first("years_since_started_at", ignorenulls=True).over(w6).alias("first_known_years_since_started_at"),
    )
)

stats = (
    enriched
    .groupBy("session_no")
    .agg(
        F.max(F.when(F.col("browser") == "Firefox", F.col("web_event_duration_sec"))).alias("firefox_max_web_event_duration_sec"),
        F.percentile_approx("web_event_duration_sec_avg_3_prior", [0.25, 0.5, 0.75]).alias("web_event_duration_sec_avg_3_prior_quartiles"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Safari", 1)), F.lit(0)), 2).alias("session_no_per_safari"),
    )
)

result = (
    stats
    .select(
        "session_no",
        "firefox_max_web_event_duration_sec",
        "web_event_duration_sec_avg_3_prior_quartiles",
        "session_no_per_safari",
    )
    .orderBy("session_no")
)
