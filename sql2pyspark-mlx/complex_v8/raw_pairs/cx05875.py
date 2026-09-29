from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_artist_country_artist_id = Window.partitionBy("artist_country", "artist_id")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_duration_sec_dense_rank_2_by = Window.partitionBy("duration_sec_dense_rank_2").orderBy("first_bpm", "id").rowsBetween(-3, -1)
w_bpm_dense_rank_release_date = Window.partitionBy("bpm_dense_rank", "release_date_tercile")
w_duration_sec_dense_rank_2_by_2 = Window.partitionBy("duration_sec_dense_rank_2").orderBy("first_bpm", "id")

prepared = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_details_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isNotNull()), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        F.col("a.country").alias("artist_country"),
    )
)

ranked = (
    with_details_artist
    .select(
        "id",
        "duration_sec",
        "moods",
        F.dense_rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_dense_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.dense_rank().over(w_artist_id_by_bpm_ties).alias("bpm_dense_rank"),
        F.round(F.avg("bpm").over(w_artist_country_artist_id), 2).alias("artist_country_artist_id_avg_bpm"),
        F.dense_rank().over(w_all_by_duration_sec_ties).alias("duration_sec_dense_rank_2"),
        F.lead("duration_sec", 2).over(w_artist_id_by_duration_sec).alias("next2_duration_sec"),
        F.first("bpm").over(w_artist_id_by_bpm_full).alias("first_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.count(F.lit(1)).over(w_artist_country_artist_id).alias("rows_in_artist_country_artist_id"),
        F.ntile(3).over(w_all_by_release_date).alias("release_date_tercile"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec_ti), 3).alias("duration_sec_percent_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_country_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_country_artist_id"),
        F.nth_value("release_date", 2).over(w_artist_id_by_bpm_full).alias("second_release_date"),
    )
)

leaders = (
    ranked
    .filter(F.col("duration_sec_dense_rank") <= 3)
    .drop("duration_sec_dense_rank")
)

windowed = (
    leaders
    .select(
        "id",
        "duration_sec",
        "moods",
        "rows_in_artist_id",
        "bpm_dense_rank",
        "next2_duration_sec",
        "first_bpm",
        "release_date_tercile",
        "duration_sec_percent_rank",
        "duration_sec_share_of_artist_country_artist_id",
        "second_release_date",
        F.max("duration_sec_dense_rank_2").over(w_duration_sec_dense_rank_2_by).alias("duration_sec_dense_rank_2_max_3_prior"),
        F.round(F.col("duration_sec_dense_rank_2") / F.nullif(F.sum("duration_sec_dense_rank_2").over(w_bpm_dense_rank_release_date), F.lit(0)), 4).alias("duration_sec_dense_rank_2_share_of_bpm_dense_ran"),
        (F.col("duration_sec_percent_rank") - F.lag("duration_sec_percent_rank").over(w_duration_sec_dense_rank_2_by_2)).alias("duration_sec_percent_rank_change"),
        F.round(F.avg("release_date_tercile").over(w_bpm_dense_rank_release_date), 2).alias("bpm_dense_rank_release_date_tercile_avg_release_"),
    )
)

result = (
    windowed
    .filter(F.col("rows_in_artist_id") > 4)
    .select(
        "id",
        "moods",
        "rows_in_artist_id",
        "bpm_dense_rank",
        "first_bpm",
        "release_date_tercile",
        "duration_sec_percent_rank",
        "duration_sec_share_of_artist_country_artist_id",
        "second_release_date",
        "duration_sec_dense_rank_2_max_3_prior",
        "bpm_dense_rank_release_date_tercile_avg_release_",
        F.date_format(F.col("second_release_date"), "MMM yyyy").alias("second_release_date_label"),
        F.date_format(F.col("second_release_date"), "MMM yyyy").alias("second_release_date_label_2"),
    )
    .orderBy(
        F.col("duration_sec_dense_rank_2_max_3_prior").desc(),
        "second_release_date_label",
    )
)
