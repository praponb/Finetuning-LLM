from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy("started_at_dayofweek_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & F.hour("started_at").between(10, 13))
    .select(
        "id",
        "device",
        "browser",
        F.dayofweek("started_at").alias("started_at_dayofweek"),
    )
)

cleaned_sessions = (
    cleaned
    .filter(F.col("device") != "desktop")
    .select(
        "*",
        F.floor("started_at_dayofweek").alias("started_at_dayofweek_whole"),
        F.trim("browser").alias("clean_browser"),
        F.coalesce("browser", F.lit("DIRECT")).alias("resolved_browser"),
    )
)

with_windows = (
    cleaned_sessions
    .select(
        "id",
        "device",
        "browser",
        "started_at_dayofweek_whole",
        F.sum("started_at_dayofweek_whole").over(w).alias("running_sum_started_at_dayofweek_whole"),
        F.max("started_at_dayofweek_whole").over(w).alias("running_max_started_at_dayofweek_whole"),
        F.min("started_at_dayofweek_whole").over(w).alias("running_min_started_at_dayofweek_whole"),
        F.count("started_at_dayofweek_whole").over(w).alias("running_count_started_at_dayofweek_whole"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.event_type"))
)

latest_match_sessions = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time").alias("web_event_event_time"))
)

result = (
    latest_match_sessions
    .groupBy(
        F.when(F.col("running_min_started_at_dayofweek_whole") >= 61, "PREMIUM").otherwise("STANDARD").alias("running_min_started_at_dayofweek_whole_tier"),
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("running_min_started_at_dayofweek_whole") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("running_min_started_at_dayofweek_whole_per_desktop"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("running_min_started_at_dayofweek_whole_per_desktop").desc())
)
