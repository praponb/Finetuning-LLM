from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type")
w_grp = Window.partitionBy("session_id", "event_type")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-04-06 16:00:00"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

prev_events = cleaned.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

metrics = (
    sessions_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "event_type_prefix",
        "session_start_flag",
        "session_id_2",
        F.max("session_start_flag").over(w3).alias("event_type_max_session_start_flag"),
        F.avg("duration_sec").over(w_grp).alias("session_id_avg_duration_sec"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.event_time",
        "src.session_start_flag",
        "src.event_type_max_session_start_flag",
        F.col("b.browser"),
        F.col("b.device"),
        F.col("b.country"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "session_start_flag",
        "event_type_max_session_start_flag",
        "browser",
        "device",
        "country",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("max_web_event_duration_sec").desc(), "country")
)
