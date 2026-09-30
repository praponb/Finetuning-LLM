from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_genre_artist_id_by_bp = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_artist_genre_artist_id = Window.partitionBy("artist_genre", "artist_id")
w_artist_genre_artist_id_by_bp_2 = Window.partitionBy("artist_genre", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-3, -1)

cleaned = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "Pop")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        F.col("a.genre").alias("artist_genre"),
    )
)

metrics = (
    joined
    .select(
        "id",
        "artist_id",
        F.sum("bpm").over(w_artist_genre).alias("artist_genre_sum_bpm"),
        F.lag("bpm", 1, 0).over(w_artist_id_by_bpm).alias("prev_bpm"),
        F.row_number().over(w_artist_genre_artist_id_by_bp).alias("bpm_seq"),
        F.lead("bpm", 3, 0).over(w_artist_id_by_bpm).alias("next3_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_genre_artist_id), 2).alias("bpm_vs_artist_genre_artist_id_avg"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_genre), 2).alias("bpm_vs_artist_genre_avg"),
        F.min("bpm").over(w_artist_genre_artist_id).alias("artist_genre_artist_id_min_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_genre_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_genre_artist_id"),
        F.round(F.avg("bpm").over(w_artist_genre_artist_id_by_bp_2), 2).alias("bpm_avg_3_prior"),
    )
)

top_rows = (
    metrics
    .filter((F.col("bpm_seq") <= 5) & (F.col("bpm_vs_artist_genre_avg") > -54.0))
    .drop("bpm_seq")
)

result = (
    top_rows
    .select(
        "id",
        "artist_genre_sum_bpm",
        "prev_bpm",
        "next3_bpm",
        "bpm_vs_artist_genre_artist_id_avg",
        "bpm_vs_artist_genre_avg",
        "artist_genre_artist_id_min_bpm",
        "bpm_avg_3_prior",
        F.coalesce(F.col("bpm_avg_3_prior"), F.lit(0.0)).alias("bpm_avg_3_prior_filled"),
    )
    .orderBy(F.col("bpm_avg_3_prior_filled").desc(), "id")
)
