from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_plays = df_plays.filter(F.col("source").isNotNull())

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_track_id_running))

result = (
    sessionized
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "starts_session",
        "burst_id",
        F.round(F.col("ms_played") / F.nullif(F.col("starts_session"), F.lit(0)), 3).alias("ms_played_per_starts_session"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("prev_played_at"))) / 60, 1).alias("minutes_prev_played_at_to_played_at"),
    )
    .orderBy(F.col("starts_session").desc_nulls_first(), "id")
    .limit(20)
)
