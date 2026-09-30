from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_first_track_release_date_mon = Window.partitionBy(F.date_trunc("month", "first_track_release_date"), "artist_genre").orderBy(F.col("track_count").desc(), "id")
w_track_count = Window.partitionBy("track_count")
w_first_track_release_date_mon_2 = Window.partitionBy(F.date_trunc("month", "first_track_release_date"), "artist_genre").orderBy(F.col("track_count").desc())

recent_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("P%"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

with_parent = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(~F.col("a.genre").isin("HipHop", "Classical"))
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.title_slug",
        "s.bpm_bucket",
        F.col("a.genre").alias("artist_genre"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
    )
)

with_stats = (
    with_parent.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

sequenced = (
    with_stats
    .select(
        "*",
        F.round(F.percent_rank().over(w_first_track_release_date_mon), 3).alias("track_count_percent_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_track_count), F.lit(0)), 4).alias("bpm_share_of_track_count"),
        F.sum("track_count").over(w_track_count).alias("track_count_sum_track_count"),
        F.rank().over(w_first_track_release_date_mon_2).alias("track_count_rank"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["track_count_rank", "artist_id", "artist_genre"], ["track_count_rank"], []],
        "track_count_rank", "artist_id", "artist_genre",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
    )
    .orderBy(
        F.col("track_count_rank").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
        F.col("artist_genre").asc_nulls_last(),
    )
)
