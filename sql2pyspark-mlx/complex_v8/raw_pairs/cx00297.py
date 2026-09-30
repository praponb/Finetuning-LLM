from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy("duration_sec", "id")
w_all_by_release_date = Window.orderBy(F.col("release_date").asc_nulls_last(), "id")
w_all_by_release_date_prior_3 = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_artist_id_release_date_year_2 = Window.partitionBy("artist_id", F.year("release_date")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_artist_id_release_date_year_3 = Window.partitionBy("artist_id", F.year("release_date")).orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)

prepared_tracks = df_tracks.filter(F.col("duration_sec").between(165, 402))

ranked = (
    prepared_tracks
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_release_date_year)).alias("days_since_prev_release_date"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_release_date)).alias("duration_sec_change"),
        F.round(F.avg("duration_sec").over(w_all_by_release_date_prior_3), 2).alias("duration_sec_avg_3_prior"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.nth_value("duration_sec", 2).over(w_artist_id_release_date_year_2).alias("second_duration_sec"),
        (F.col("bpm") - F.lag("bpm").over(w_is_explicit_by_release_date)).alias("bpm_change"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_year_3), 2).alias("bpm_avg_4"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        F.coalesce(F.col("rows_in_artist_id"), F.lit(0)).alias("rows_in_artist_id_filled"),
        F.when(F.col("bpm_vs_artist_id_avg") >= 36.0, "HIGH").when(F.col("bpm_vs_artist_id_avg") >= -36.0, "MEDIUM").otherwise("LOW").alias("bpm_vs_artist_id_avg_tier"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

grouped = (
    labeled
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        "is_explicit",
    )
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("R%"), 1).otherwise(0)).alias("n_upper_trim_title_like_r"),
        F.count("*").alias("cnt"),
        F.round(F.sum("artist_id_avg_duration_sec") / F.nullif(F.sum("second_duration_sec"), F.lit(0)), 3).alias("artist_id_avg_duration_sec_to_second_duration_se"),
        F.sum("title_len").alias("total_title_len"),
        F.round(F.avg("duration_sec_change"), 2).alias("avg_duration_sec_change"),
    )
)

result = (
    grouped
    .select(
        "release_date_month",
        "n_upper_trim_title_like_r",
        "artist_id_avg_duration_sec_to_second_duration_se",
        "avg_duration_sec_change",
        F.coalesce(F.col("avg_duration_sec_change"), F.lit(0.0)).alias("avg_duration_sec_change_filled"),
    )
    .orderBy(
        "artist_id_avg_duration_sec_to_second_duration_se",
        F.col("release_date_month").desc(),
    )
)
