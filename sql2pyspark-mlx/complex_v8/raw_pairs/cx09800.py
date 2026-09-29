from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_prior_6 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("artist_debut_year").asc_nulls_last())
w_artist_id_by_bpm_moving_6 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_is_verified_is_explic_2 = Window.partitionBy("artist_is_verified", "is_explicit").orderBy(F.col("duration_sec").desc())
w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc())
w_artist_id_artist_is_verified_2 = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id")
w_artist_is_verified_is_explic_3 = Window.partitionBy("artist_is_verified", "is_explicit")
w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy("artist_debut_year", "id").rowsBetween(-3, -1)

cleaned = (
    df_tracks
    .filter(F.col("bpm") <= 127)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

combined_details_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

metrics = (
    combined_details_artist
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "artist_is_verified",
        "artist_debut_year",
        F.sum("duration_sec").over(w_artist_id_by_bpm_prior_6).alias("duration_sec_sum_6_prior"),
        F.round(F.cume_dist().over(w_artist_id_artist_is_verified), 3).alias("artist_debut_year_cume_dist"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_bpm_moving_6), 2).alias("duration_sec_avg_7"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_artist_id_by_duration_sec)).alias("artist_debut_year_change"),
        F.ntile(5).over(w_artist_is_verified_is_explic).alias("duration_sec_quintile"),
        F.row_number().over(w_artist_id_by_bpm).alias("bpm_seq"),
        F.round(F.percent_rank().over(w_artist_is_verified_is_explic_2), 3).alias("duration_sec_percent_rank"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_duration_sec_mo), 2).alias("duration_sec_avg_2"),
        F.lag("duration_sec", 2).over(w_all_by_duration_sec).alias("prev2_duration_sec"),
        F.dense_rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_dense_rank"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_artist_is_verified_2)).alias("duration_sec_change"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_is_verified_is_explic_3), F.lit(0)), 4).alias("duration_sec_share_of_artist_is_verified_is_expl"),
        F.sum("duration_sec").over(w_artist_is_verified_artist_id).alias("duration_sec_sum_3_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "title",
        "duration_sec",
        "is_explicit",
        "artist_is_verified",
        "artist_debut_year_cume_dist",
        "duration_sec_avg_7",
        "duration_sec_quintile",
        "duration_sec_percent_rank",
        "duration_sec_avg_2",
        "prev2_duration_sec",
        "duration_sec_share_of_artist_is_verified_is_expl",
        "duration_sec_sum_3_prior",
        (F.floor(F.col("duration_sec_avg_7") / 50) * 50).alias("duration_sec_avg_7_bucket"),
    )
    .orderBy("title")
)
