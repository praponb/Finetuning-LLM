from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_artist_id = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("duration_sec", "id")
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy("duration_sec", "id")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date")).orderBy("bpm", "id")

scoped = (
    df_tracks
    .filter(F.year(F.col("release_date")) == 2024)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.add_months(F.col("release_date"), 6).alias("release_date_plus_6m"),
    )
)

windowed = (
    scoped
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_plus_6m",
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.min("duration_sec").over(w_is_explicit).alias("is_explicit_min_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("rn"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.dense_rank().over(w_release_date_month_artist_id).alias("duration_sec_dense_rank"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_release_date_year)).alias("days_since_prev_release_date"),
        F.datediff(F.col("release_date_plus_6m"), F.lag("release_date_plus_6m").over(w_artist_id_release_date_month)).alias("days_since_prev_release_date_plus_6m"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        (F.col("artist_id_max_bpm") + F.col("duration_sec_dense_rank")).alias("artist_id_max_bpm_plus_duration_sec_dense_rank"),
        (F.col("bpm_share_of_is_explicit") + F.col("rn")).alias("bpm_share_of_is_explicit_plus_rn"),
    )
)

result = (
    annotated
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "distinct_artist_id_in_is_explicit",
        "duration_sec_share_of_is_explicit",
        "rn",
        "artist_id_max_bpm",
        "duration_sec_dense_rank",
        "days_since_prev_release_date_plus_6m",
        "artist_id_max_bpm_plus_duration_sec_dense_rank",
        (F.col("rn") - F.col("bpm_share_of_is_explicit")).alias("rn_minus_bpm_share_of_is_explicit"),
    )
)
