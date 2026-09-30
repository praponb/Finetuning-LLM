from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

with_prev = scoped_plays.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_device_running))

session_summary = (
    sessionized
    .groupBy("device", "session_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("ms_played").alias("session_ms_played"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    session_summary
    .select(
        "*",
        F.when(F.col("device").isin("speaker", "tv"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
    )
)

result = (
    annotated
    .select(
        "device",
        "session_id",
        F.col("session_ms_played").alias("session_ms_played_v"),
        F.col("event_count").alias("m_event_count"),
    )
    .unpivot(
        ids=["device", "session_id"],
        values=["session_ms_played_v", "m_event_count"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("device", "session_id", "field")
)
