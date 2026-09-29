from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("device").isin("speaker", "phone"))
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

with_prev = valid.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_device_running))

session_summary = (
    sessionized
    .groupBy("device", "session_no")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "device",
        "session_start",
        "session_end",
        "n_events",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.round(F.col("session_no") / F.nullif(F.col("duration_min"), F.lit(0)), 3).alias("session_no_per_duration_min"),
    )
    .orderBy(F.col("session_end").desc(), F.col("session_start").desc())
)
