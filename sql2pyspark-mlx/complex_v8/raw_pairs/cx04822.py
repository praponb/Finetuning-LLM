from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped = Window.partitionBy("source", "is_skipped")
w_all_by_played_at = Window.orderBy("played_at", "id")

base = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.concat(F.col("device"), F.lit("#"), F.col("track_id").cast("string")).alias("device_ref"),
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
    )
)

joined = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("l.country").isin("BR", "TH"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.device_ref",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.country").alias("listener_country"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

windowed = (
    joined
    .select(
        "*",
        F.sum("track_bpm").over(w_source_is_skipped).alias("source_is_skipped_sum_track_bpm"),
        F.count(F.lit(1)).over(w_source_is_skipped).alias("rows_in_source_is_skipped"),
        F.lag("track_bpm").over(w_all_by_played_at).alias("prev_track_bpm"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source_is_skipped), 2).alias("ms_played_vs_source_is_skipped_avg"),
    )
)

joined_2 = (
    windowed.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.device_ref",
        "s.listener_interests",
        "s.listener_country",
        "s.track_bpm",
        "s.track_release_date",
        "s.source_is_skipped_sum_track_bpm",
        "s.rows_in_source_is_skipped",
        "s.prev_track_bpm",
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    joined_2
    .filter(F.col("source").isin("search", "album", "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "device_ref",
        "listener_interests",
        "listener_country",
        "track_bpm",
        "track_release_date",
        "source_is_skipped_sum_track_bpm",
        "prev_track_bpm",
        "listener_plan",
        F.year(F.col("track_release_date")).alias("track_release_date_year"),
        (F.col("prev_track_bpm") - F.col("track_bpm")).alias("prev_track_bpm_minus_track_bpm"),
    )
    .orderBy(F.col("prev_track_bpm").desc(), "listener_id")
)
