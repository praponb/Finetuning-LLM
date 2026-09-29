from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_ms_played = Window.orderBy("max_ms_played", "is_skipped", "track_id")
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_all_by_max_ms_played_moving = Window.orderBy("max_ms_played", "is_skipped", "track_id").rowsBetween(-3, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

grouped = (
    valid
    .groupBy("is_skipped", "track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.sum("ms_played").alias("total_ms_played"),
        F.max("ms_played").alias("max_ms_played"),
    )
)

windowed = (
    grouped
    .select(
        "is_skipped",
        "track_id",
        "first_played_at",
        "total_ms_played",
        F.ntile(10).over(w_all_by_max_ms_played).alias("max_ms_played_decile"),
        F.count(F.lit(1)).over(w_track_id_is_skipped).alias("rows_in_track_id_is_skipped"),
        F.max("total_ms_played").over(w_all_by_max_ms_played_moving).alias("total_ms_played_max_4"),
    )
)

with_parent = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.is_skipped",
        "s.track_id",
        "s.first_played_at",
        "s.max_ms_played_decile",
        "s.rows_in_track_id_is_skipped",
        "s.total_ms_played_max_4",
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_parent
    .filter(~F.col("is_skipped"))
    .select(
        "is_skipped",
        "first_played_at",
        "max_ms_played_decile",
        "rows_in_track_id_is_skipped",
        "total_ms_played_max_4",
        "track_title",
        "track_duration_sec",
        "artist_debut_year",
    )
    .orderBy(F.col("track_duration_sec").desc(), "track_title")
)
