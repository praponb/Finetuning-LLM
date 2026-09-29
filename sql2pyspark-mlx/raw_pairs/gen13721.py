from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("duration_sec_rounded", "id")
w3 = Window.partitionBy("session_browser", "event_type").orderBy("duration_sec_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.to_date("event_time") >= "2024-12-04"))
    .select(
        "*",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.round("duration_sec", 0).alias("duration_sec_rounded_2"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device") == "tablet")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.duration_sec",
        "a.duration_sec_rounded",
        "a.duration_sec_rounded_2",
        F.col("d.browser"),
        F.col("d.device"),
        F.col("d.country"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.event_type",
        "a.duration_sec_rounded",
        "a.device",
        "a.country",
        F.col("b.device").alias("session_device"),
        F.col("b.browser").alias("session_browser"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec_rounded",
        "device",
        "session_browser",
        F.min("duration_sec_rounded").over(w).alias("country_min_duration_sec_rounded"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("device", ignorenulls=True).over(w3).alias("last_known_device"),
    )
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "device",
        "session_browser",
        "seq_no",
        "last_known_device",
        F.col("avg_web_event_duration_sec"),
    )
)
