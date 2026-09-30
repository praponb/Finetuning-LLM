from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "browser").orderBy("duration_sec_whole", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("session_browser").orderBy("duration_sec_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.month("event_time"), "session_browser").orderBy("duration_sec", "id")
w4 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-5, -1)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.country").isin(["SG", "US"]))
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.event_time",
        "a.duration_sec",
        F.col("d.browser"),
        F.col("d.country"),
    )
)

cleaned = (
    joined
    .filter(F.col("event_time") > F.to_date(F.lit("2023-04-27")))
    .select(
        "*",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.country").alias("session_country"),
        F.col("p.browser").alias("session_browser"),
        F.col("p.device"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "country",
        "browser_title",
        "resolved_browser",
        "session_country",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_2_prior"),
        F.first("session_browser", ignorenulls=True).over(w2).alias("first_known_session_browser"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
        F.avg("duration_sec").over(w4).alias("duration_sec_avg_5_prior"),
    )
)

pivoted = (
    scored
    .groupBy("session_id")
    .pivot("country", ["SG", "US"])
    .agg(
        F.sum("duration_sec_avg_5_prior").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)

result = (
    pivoted
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "SG_amount",
        "US_item_count",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("SG_amount").asc_nulls_last())
)
