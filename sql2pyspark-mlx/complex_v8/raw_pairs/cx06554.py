from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_source_by_played = Window.partitionBy("is_skipped", "source").orderBy(F.col("played_at").desc())
w_track_artist_id_track_id = Window.partitionBy("track_artist_id", "track_id")
w_is_skipped_source_by_played_2 = Window.partitionBy("is_skipped", "source").orderBy(F.col("played_at").desc(), "id")

base = (
    df_plays
    .filter(F.col("device") == "phone")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.bpm") >= 85)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_skipped_source_by_played), 3).alias("played_at_cume_dist"),
        F.size(F.collect_set("source").over(w_track_artist_id_track_id)).alias("distinct_source_in_track_artist_id_track_id"),
        F.ntile(4).over(w_is_skipped_source_by_played_2).alias("played_at_quartile"),
        (F.col("ms_played") - F.lag("ms_played").over(w_is_skipped_source_by_played_2)).alias("ms_played_change"),
    )
)

derived = (
    metrics
    .filter(F.size(F.col("track_moods")) >= 2)
    .select(
        "*",
        F.abs(F.col("ms_played_change") - -120000).alias("ms_played_change_distance"),
        F.round(F.col("played_at_cume_dist") * 0.9, 2).alias("played_at_cume_dist_adj"),
    )
)

tagged = (
    derived
    .filter(F.col("ms_played") <= 282311)
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "track_artist_id",
        "played_at_cume_dist",
        "ms_played_change",
        "ms_played_change_distance",
        "played_at_cume_dist_adj",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

result = (
    tagged
    .select(
        "id",
        "track_artist_id",
        "played_at_cume_dist",
        "ms_played_change",
        "ms_played_change_distance",
        "played_at_cume_dist_adj",
        "days_since_played_at",
        "played_at_hour",
    )
    .orderBy(F.col("ms_played_change").desc(), F.col("played_at_hour").desc())
)
