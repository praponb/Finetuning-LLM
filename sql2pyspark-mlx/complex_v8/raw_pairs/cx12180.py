from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_artist_genre_artist_is_verif = Window.partitionBy("artist_genre", "artist_is_verified").orderBy("release_date", "id").rowsBetween(-3, -1)
w_all_by_release_date_moving_6 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_genre_artist_is_verif_2 = Window.partitionBy("artist_genre", "artist_is_verified").orderBy("release_date", "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_release_date_yea = Window.partitionBy("is_explicit", F.year("release_date")).orderBy("release_date")
w_artist_genre_artist_is_verif_3 = Window.partitionBy("artist_genre", "artist_is_verified").orderBy("release_date")
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy("release_date_dense_rank", "id")
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified", "artist_id").orderBy("release_date_dense_rank")
w_release_date_dense_rank_2_ar = Window.partitionBy("release_date_dense_rank_2", "artist_genre")
w_artist_id_seq_no_2_by_durati = Window.partitionBy("artist_id", "seq_no_2").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter((F.col("bpm").between(73, 166)) & (F.col("artist_id") >= 9))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

enriched = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.least_bpm_duration_sec",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_genre).alias("rows_in_artist_genre"),
        F.round(F.avg("duration_sec").over(w_artist_genre_artist_is_verif), 2).alias("duration_sec_avg_3_prior"),
        F.max("least_bpm_duration_sec").over(w_all_by_release_date_moving_6).alias("least_bpm_duration_sec_max_7"),
        F.row_number().over(w_artist_genre_artist_is_verif_2).alias("seq_no"),
        F.round(F.avg("bpm").over(w_is_explicit), 2).alias("is_explicit_avg_bpm"),
        F.dense_rank().over(w_is_explicit_release_date_yea).alias("release_date_dense_rank"),
        F.dense_rank().over(w_artist_genre_artist_is_verif_3).alias("release_date_dense_rank_2"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.row_number().over(w_all_by_release_date).alias("seq_no_2"),
        F.rank().over(w_is_explicit_release_date_yea).alias("release_date_rank"),
    )
)

scored = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "least_bpm_duration_sec",
        "artist_is_verified",
        "artist_genre",
        "least_bpm_duration_sec_max_7",
        "seq_no",
        "is_explicit_avg_bpm",
        "release_date_dense_rank",
        "release_date_dense_rank_2",
        "bpm_share_of_is_explicit",
        "seq_no_2",
        "release_date_rank",
        F.initcap(F.trim(F.col("artist_genre"))).alias("artist_genre_clean"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.lead("seq_no_2").over(w_artist_is_verified_artist_id).alias("next_seq_no_2"),
        F.round(F.cume_dist().over(w_artist_is_verified_artist_id_2), 3).alias("release_date_dense_rank_cume_dist"),
        F.size(F.collect_set("artist_id").over(w_release_date_dense_rank_2_ar)).alias("distinct_artist_id_in_release_date_dense_rank_2_"),
        F.sum("release_date_dense_rank_2").over(w_release_date_dense_rank_2_ar).alias("release_date_dense_rank_2_artist_genre_sum_relea"),
        F.dense_rank().over(w_artist_is_verified_artist_id).alias("release_date_dense_rank_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id_seq_no_2_by_durati).alias("running_count"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "duration_sec",
        "bpm",
        "artist_is_verified",
        "artist_genre",
        "is_explicit_avg_bpm",
        "release_date_dense_rank",
        "release_date_dense_rank_2",
        "bpm_share_of_is_explicit",
        "seq_no_2",
        "artist_genre_clean",
        "next_seq_no_2",
        "release_date_dense_rank_cume_dist",
        "distinct_artist_id_in_release_date_dense_rank_2_",
        "release_date_dense_rank_dense_rank",
        "running_count",
    )
)
