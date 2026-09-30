from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_track_artist_id_by = Window.partitionBy("track_id", "track_artist_id").orderBy(F.col("track_bpm").desc(), "id", "track_id")
w_first_track_release_date_mon = Window.partitionBy(F.date_trunc("month", "first_track_release_date"), "track_id")
w_track_id_track_artist_id_by_2 = Window.partitionBy("track_id", "track_artist_id").orderBy(F.col("track_bpm").desc(), "id", "track_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        F.length(F.trim(F.col("country"))).alias("country_len"),
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
    )
)

line_level_tracks = (
    recent.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.release_date").isNotNull()), "inner")
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.country_len",
        F.col("t.id").alias("track_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_counts = (
    line_level_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_details_track = (
    with_counts.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.array_contains(F.col("t.moods"), "workout"))
    .select(
        "s.id",
        "s.artist_name",
        "s.country",
        "s.country_len",
        "s.track_id",
        "s.track_bpm",
        "s.track_title",
        "s.track_release_date",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.moods").alias("track_moods"),
    )
)

scored = (
    with_details_track
    .select(
        "id",
        "artist_name",
        "track_id",
        "track_bpm",
        "track_title",
        "track_release_date",
        "track_artist_id",
        F.date_trunc("quarter", F.col("track_release_date")).alias("track_release_date_quarter"),
        F.substring(F.trim(F.col("track_title")), 1, 3).alias("track_title_prefix"),
        F.abs(F.col("track_bpm") - 89).alias("track_bpm_distance"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

with_counts_2 = (
    scored.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_track_release_date")
)

with_windows = (
    with_counts_2
    .select(
        "*",
        F.ntile(3).over(w_track_id_track_artist_id_by).alias("track_bpm_tercile"),
        F.round(F.col("track_bpm_distance") / F.nullif(F.sum("track_bpm_distance").over(w_first_track_release_date_mon), F.lit(0)), 4).alias("track_bpm_distance_share_of_first_track_release_"),
        F.last("track_bpm").over(w_track_id_track_artist_id_by_2).alias("last_track_bpm"),
        F.dense_rank().over(w_track_id_track_artist_id_by).alias("track_bpm_dense_rank"),
        F.min("track_bpm").over(w_first_track_release_date_mon).alias("first_track_release_date_month_track_id_min_trac"),
    )
)

result = (
    with_windows
    .filter(F.upper(F.trim(F.col("track_title"))).like("M%"))
    .select(
        "id",
        "track_bpm",
        "track_artist_id",
        "track_release_date_quarter",
        "track_title_prefix",
        "track_bpm_distance",
        "first_track_release_date",
        "track_bpm_tercile",
        "track_bpm_distance_share_of_first_track_release_",
        "last_track_bpm",
        F.substring(F.trim(F.col("artist_name")), 1, 3).alias("artist_name_prefix"),
    )
    .orderBy(F.col("last_track_bpm").asc(), "artist_name_prefix")
)
