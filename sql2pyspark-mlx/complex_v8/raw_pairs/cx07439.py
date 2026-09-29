from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_artist_id_by_ma = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("max_duration_sec").desc(), "release_date_month", "artist_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_genre_artist_id_by_ma_2 = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("max_duration_sec").desc(), "release_date_month", "artist_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_artist_id_by_ma_3 = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("max_duration_sec").desc(), "release_date_month", "artist_id")

filtered = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-07-01").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

per_group = (
    filtered
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        "artist_id",
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum("days_since_release_date").alias("total_days_since_release_date"),
        F.count("*").alias("row_count"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("row_count") >= 2)
)

with_parent_artist = (
    per_group.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isNotNull()), "left")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

ranked = (
    with_parent_artist
    .select(
        "*",
        F.last("row_count").over(w_artist_genre_artist_id_by_ma).alias("last_row_count"),
        F.min("row_count").over(w_artist_genre_artist_id_by_ma_2).alias("min_row_count_so_far"),
        F.lag("avg_duration_sec", 2).over(w_artist_genre_artist_id_by_ma_3).alias("prev2_avg_duration_sec"),
    )
)

filtered_2 = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") > 94), "left_anti")
)

screened = (
    filtered_2.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 96) & (F.col("t.release_date") >= F.lit("2024-11-06").cast("date")), "left_semi")
)

result = (
    screened
    .groupBy("row_count", "release_date_month")
    .pivot("artist_genre", ["Indie", "Pop", "Jazz"])
    .agg(F.sum("avg_duration_sec"))
)
