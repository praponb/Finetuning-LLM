from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_count_2 = Window.partitionBy("track_count_2")

base_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

filtered = (
    base_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isin("car", "phone")), "left_anti")
    .filter(F.col("duration_sec") >= 184)
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"), F.count("*").alias("track_count_2"))
)

with_counts = (
    filtered.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

with_activity = (
    with_counts.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.size(F.col("t.moods")) >= 2), "left_anti")
)

with_windows = (
    with_activity
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "greatest_duration_sec_bpm",
        "track_count",
        "track_count_2",
        F.max("track_count_2").over(w_track_count_2).alias("track_count_2_max_track_count_2"),
        F.round(F.col("track_count_2") - F.avg("track_count_2").over(w_track_count_2), 2).alias("track_count_2_vs_track_count_2_avg"),
    )
)

combined_details_artist = (
    with_windows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("K%"))
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.greatest_duration_sec_bpm",
        "s.track_count_2",
        "s.track_count_2_max_track_count_2",
        "s.track_count_2_vs_track_count_2_avg",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

filtered_2 = (
    combined_details_artist
    .crossJoin(combined_details_artist.agg(F.avg("track_count_2_max_track_count_2").alias("overall_avg_track_count_2_max_track_count_2")))
    .filter(F.col("track_count_2_max_track_count_2") > F.col("overall_avg_track_count_2_max_track_count_2"))
    .drop("overall_avg_track_count_2_max_track_count_2")
)

result = (
    filtered_2
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "greatest_duration_sec_bpm",
        "track_count_2",
        "track_count_2_max_track_count_2",
        "track_count_2_vs_track_count_2_avg",
        "artist_genre",
        F.lower(F.regexp_replace(F.trim(F.col("artist_genre")), " ", "_")).alias("artist_genre_slug"),
    )
    .orderBy(F.col("artist_genre").desc_nulls_first())
)
