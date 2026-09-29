from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")

cleaned = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.max("duration_sec").over(w_release_date_year_artist_id).alias("duration_sec_max_4"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.duration_sec_max_4",
        "s.bpm_vs_artist_id_avg",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    joined
    .groupingSets(
        [["artist_is_verified", "artist_id"], ["artist_is_verified"]],
        "artist_is_verified", "artist_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.round(F.sum("bpm_vs_artist_id_avg"), 2).alias("total_bpm_vs_artist_id_avg"),
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
    )
    .orderBy(
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
