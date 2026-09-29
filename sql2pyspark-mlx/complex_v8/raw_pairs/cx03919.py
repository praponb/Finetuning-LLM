from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_is_explicit_artist_id_by_bpm = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id")
w_is_explicit_artist_id_by_bpm_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)

recent = (
    df_tracks
    .filter(F.col("bpm") <= 142)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

with_trends = (
    recent
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_release_date_month), F.lit(0)), 4).alias("bpm_share_of_artist_id_release_date_month"),
        F.row_number().over(w_is_explicit_artist_id_by_bpm).alias("bpm_seq"),
        F.max("bpm").over(w_is_explicit_artist_id_by_bpm_2).alias("bpm_max_6_prior"),
        F.rank().over(w_is_explicit_artist_id_by_bpm).alias("bpm_rank"),
        F.max("bpm").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_max_bpm"),
        F.lag("bpm").over(w_is_explicit_artist_id_by_bpm).alias("prev_bpm"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["is_explicit", "bpm_seq", "bpm_rank"], ["is_explicit"], ["bpm_seq"], []],
        "is_explicit", "bpm_seq", "bpm_rank",
    )
    .agg(F.min("bpm_max_6_prior").alias("min_bpm_max_6_prior"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("bpm_seq").asc_nulls_last(),
        F.col("bpm_rank").asc_nulls_last(),
    )
)
