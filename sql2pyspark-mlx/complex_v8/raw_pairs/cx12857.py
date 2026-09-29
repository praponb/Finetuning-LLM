from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("duration_sec").desc())
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc())
w_all_by_duration_sec_vs_artis = Window.orderBy(F.col("duration_sec_vs_artist_id_avg").desc(), "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 409)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

windowed = (
    prepared_tracks
    .select(
        "*",
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.round(F.percent_rank().over(w_artist_id_release_date_year), 3).alias("duration_sec_percent_rank"),
        F.nth_value("bpm", 2).over(w_artist_id_by_bpm_full).alias("second_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.first("duration_sec").over(w_artist_id_by_bpm_full).alias("first_duration_sec"),
        F.lag("duration_sec").over(w_artist_id_by_duration_sec).alias("prev_duration_sec"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.lead("bpm").over(w_artist_id_by_bpm).alias("next_bpm"),
        F.dense_rank().over(w_artist_id_by_duration_sec_ti).alias("duration_sec_dense_rank"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.ntile(3).over(w_all_by_duration_sec_vs_artis).alias("duration_sec_vs_artist_id_avg_tercile"),
        F.max("first_duration_sec").over(w_release_date_month).alias("release_date_month_max_first_duration_sec"),
        F.round(F.col("artist_id_max_bpm") / F.nullif(F.sum("artist_id_max_bpm").over(w_release_date_month), F.lit(0)), 4).alias("artist_id_max_bpm_share_of_release_date_month"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "release_date_label",
        "artist_id_max_bpm",
        "duration_sec_share_of_artist_id",
        "second_bpm",
        "bpm_share_of_artist_id",
        "rows_in_artist_id",
        "first_duration_sec",
        "next_bpm",
        "duration_sec_dense_rank",
        "artist_id_max_bpm_share_of_release_date_month",
        F.lower(F.trim(F.col("release_date_label"))).alias("release_date_label_clean"),
        F.round(F.col("duration_sec") / F.nullif(F.col("duration_sec_dense_rank"), F.lit(0)), 3).alias("duration_sec_per_duration_sec_dense_rank"),
    )
    .orderBy(F.col("second_bpm").desc_nulls_first())
)
