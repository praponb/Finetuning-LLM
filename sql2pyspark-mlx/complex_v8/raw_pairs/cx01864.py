from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_artist_is_verif = Window.partitionBy("artist_genre", "artist_is_verified")
w_artist_id_by_days_since_rele = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(-3, -1)
w_artist_id_by_days_since_rele_2 = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(-2, Window.currentRow)
w_artist_id_by_days_since_rele_3 = Window.partitionBy("artist_id").orderBy("days_since_release_date")
w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy("duration_sec_filled", "id")
w_artist_id_by_days_since_rele_4 = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tracks
    .filter((F.col("bpm") >= 76) | (F.col("duration_sec").between(183, 392)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.duration_sec_filled",
        "s.days_since_release_date",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "duration_sec_filled",
        "days_since_release_date",
        "artist_is_verified",
        F.min("bpm").over(w_artist_genre_artist_is_verif).alias("artist_genre_artist_is_verified_min_bpm"),
        F.round(F.avg("duration_sec_filled").over(w_artist_id_by_days_since_rele), 2).alias("duration_sec_filled_avg_3_prior"),
        F.round(F.avg("days_since_release_date").over(w_artist_id_by_days_since_rele_2), 2).alias("days_since_release_date_avg_3"),
        F.dense_rank().over(w_artist_id_by_days_since_rele_3).alias("days_since_release_date_dense_rank"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_genre_artist_is_verif), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_genre_artist_i"),
        F.round(F.avg("artist_debut_year").over(w_artist_genre_artist_is_verif), 2).alias("artist_genre_artist_is_verified_avg_artist_debut"),
        F.row_number().over(w_artist_is_verified_by_durati).alias("duration_sec_filled_seq"),
        F.sum("duration_sec").over(w_artist_id_by_days_since_rele_4).alias("running_duration_sec"),
        F.round(F.col("days_since_release_date") / F.nullif(F.sum("days_since_release_date").over(w_artist_genre_artist_is_verif), F.lit(0)), 4).alias("days_since_release_date_share_of_artist_genre_ar"),
        F.sum("bpm").over(w_artist_genre_artist_is_verif).alias("artist_genre_artist_is_verified_sum_bpm"),
        F.max("bpm").over(w_artist_id_by_days_since_rele_2).alias("bpm_max_3"),
    )
)

stats = (
    sequenced
    .groupBy(
        "is_explicit",
        "days_since_release_date_dense_rank",
        "duration_sec_filled_seq",
    )
    .agg(
        F.round(F.avg("duration_sec_filled_avg_3_prior"), 2).alias("avg_duration_sec_filled_avg_3_prior"),
        F.sum(F.when(~F.col("artist_is_verified"), 1).otherwise(0)).alias("n_artist_is_verified_false"),
    )
)

result = (
    stats
    .select(
        "is_explicit",
        "duration_sec_filled_seq",
        "n_artist_is_verified_false",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        (F.floor(F.col("duration_sec_filled_seq") / 2) * 2).alias("duration_sec_filled_seq_bucket"),
    )
    .orderBy("duration_sec_filled_seq_bucket")
)
