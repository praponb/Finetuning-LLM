from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("session_id_count_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type", "session_id").orderBy(F.col("running_session_id_count_duration_sec").desc_nulls_last())

with_windows = (
    df_web_events
    .select(
        "*",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.count("duration_sec").over(w2).alias("session_id_count_duration_sec"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("duration_sec_bucket").over(w3).alias("duration_sec_bucket_avg_30d"),
        F.sum("session_id_count_duration_sec").over(w4).alias("running_session_id_count_duration_sec"),
    )
)

enriched_web_events = (
    enriched
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.browser"), F.col("d.country"), F.col("d.device"))
)

cleaned = (
    enriched_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2024-02-25"))) & F.col("session_id_count_duration_sec").isNull())
    .select(
        "*",
        F.hour("event_time").alias("event_time_hour"),
        F.substring("page", 1, 3).alias("page_prefix"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .cube(
        F.when(F.col("running_session_id_count_duration_sec") >= 75, "LARGE").otherwise("SMALL").alias("running_session_id_count_duration_sec_tier"),
        F.date_trunc("YEAR", "event_time").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.max("event_time").alias("ended_at"),
        F.count(F.when(F.col("device") == "tablet", 1)).alias("tablet_rows"),
    )
    .orderBy(F.col("search_rows").desc_nulls_first())
)
