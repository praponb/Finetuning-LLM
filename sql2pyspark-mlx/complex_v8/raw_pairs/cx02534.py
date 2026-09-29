from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_genre_by_duration_sec = Window.partitionBy("artist_genre").orderBy(F.col("duration_sec").asc_nulls_last())
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("duration_sec").desc(), "id")
w_artist_genre_by_duration_sec_2 = Window.partitionBy("artist_genre").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_artist_is_verified_by_durati = Window.partitionBy("artist_is_verified").orderBy("duration_sec")
w_artist_id_artist_country_by_2 = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)

filtered = (
    df_tracks
    .filter((F.col("bpm") >= 104) & (F.length(F.trim(F.col("title"))) >= 4))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
    )
)

enriched = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.dense_rank().over(w_artist_genre_by_duration_sec).alias("duration_sec_dense_rank"),
        F.size(F.collect_set("artist_country").over(w_artist_id)).alias("distinct_artist_country_in_artist_id"),
        F.row_number().over(w_artist_id_artist_country_by).alias("seq_no"),
        F.round(F.percent_rank().over(w_artist_id_artist_country_by), 3).alias("duration_sec_percent_rank"),
        F.round(F.percent_rank().over(w_artist_genre_by_duration_sec_2), 3).alias("duration_sec_percent_rank_2"),
        F.round(F.percent_rank().over(w_artist_is_verified_by_durati), 3).alias("duration_sec_percent_rank_3"),
        F.sum("duration_sec").over(w_artist_id_artist_country_by_2).alias("duration_sec_sum_3_prior"),
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.size(F.collect_set("artist_genre").over(w_artist_id)).alias("distinct_artist_genre_in_artist_id"),
    )
)

result = (
    windowed
    .select(
        "id",
        "title",
        "duration_sec",
        "bpm",
        "artist_genre",
        "artist_is_verified",
        "bpm_vs_artist_id_avg",
        "duration_sec_dense_rank",
        "distinct_artist_country_in_artist_id",
        "duration_sec_percent_rank",
        "duration_sec_percent_rank_2",
        "duration_sec_percent_rank_3",
        "duration_sec_sum_3_prior",
        "artist_id_avg_duration_sec",
        "rows_in_artist_id",
        "distinct_artist_genre_in_artist_id",
        F.when(F.col("artist_is_verified"), 1).otherwise(0).alias("artist_is_verified_flag"),
        F.greatest(F.col("duration_sec_dense_rank"), F.col("duration_sec")).alias("greatest_duration_sec_dense_rank_duration_sec"),
    )
    .orderBy(F.col("title").desc())
)
