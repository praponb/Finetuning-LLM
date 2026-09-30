from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("session_start_flag").desc_nulls_first())
w5 = Window.partitionBy("session_id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.to_timestamp(F.lit("2023-06-15 18:00:00")))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_windows = (
    sessionized
    .select(
        "*",
        F.first("session_start_flag", ignorenulls=True).over(w3).alias("first_known_session_start_flag"),
        F.rank().over(w4).alias("session_start_flag_rank"),
        F.stddev_samp("burst_id").over(w5).alias("session_id_stddev_samp_burst_id"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.browser"), F.col("p2.country"), F.col("p2.device"))
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "resolved_page",
        "session_start_flag",
        "burst_id",
        "session_start_flag_rank",
        "session_id_stddev_samp_burst_id",
        F.when(F.col("event_type").rlike("^(click|page_view)$"), "CORE").otherwise("GROUP_B").alias("event_type_zone"),
        F.col("min_web_event_duration_sec"),
    )
)
