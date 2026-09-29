from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .filter(F.col("device") == "speaker")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

with_prev = prepared_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | (F.col("played_at") > F.col("prev_played_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_track_id_running))

result = (
    sessionized
    .filter(F.col("prev_played_at") < F.lit("2025-02-12 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "played_at",
        "starts_session",
        "burst_id",
        F.when(F.col("device").isin("tv", "phone"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
    .orderBy(F.col("listener_id").desc(), F.col("burst_id").asc(), "id")
    .limit(10)
)
