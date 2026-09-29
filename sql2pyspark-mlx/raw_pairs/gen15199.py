from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_device").orderBy("event_time_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("event_time_hour", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("session_device", "browser").orderBy("duration_sec", "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
    .withColumn("event_time_hour", F.hour("event_time"))
    .withColumn("page_clean", F.coalesce("page", F.lit("OTHER")))
)

with_dims = (
    base
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.browser"), F.col("p.device"))
)

with_dims_web_events = (
    with_dims
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.started_at") < "2025-05-01")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.duration_sec",
        "s.event_time_hour",
        "s.page_clean",
        "s.browser",
        F.col("p.device").alias("session_device"),
    )
)

with_windows = (
    with_dims_web_events
    .select(
        "*",
        F.sum("event_time_hour").over(w).alias("running_event_time_hour"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_5_prior"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    with_windows
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "browser",
        "session_device",
        "running_event_time_hour",
        "duration_sec_avg_5_prior",
        "seq_no",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "GROUP_B").otherwise("CORE").alias("browser_zone"),
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("page").desc_nulls_last())
)
