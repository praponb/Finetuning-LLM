from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("release_date")
w_release_date_month_artist_id = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id")
w_release_date_month_artist_id_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("release_date", "id").rowsBetween(-2, Window.currentRow)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_release_date_year_artist_id_2 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("release_date", "id")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_id_3 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("release_date", "id")
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_release_date_year_artist_id_3 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter((F.col("bpm") > 85) | (F.col("duration_sec") < 312))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.round(F.cume_dist().over(w_release_date_year_artist_id), 3).alias("release_date_cume_dist"),
        F.round(F.avg("duration_sec").over(w_release_date_month_artist_id), 2).alias("release_date_month_artist_id_avg_duration_sec"),
        F.sum("duration_sec").over(w_release_date_month_artist_id_2).alias("duration_sec_sum_3"),
        F.lead("duration_sec", 1, 0).over(w_artist_id_release_date_month).alias("next_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.ntile(5).over(w_all_by_duration_sec).alias("duration_sec_quintile"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_release_date_year_artist_id_2)).alias("days_since_prev_release_date"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.max("duration_sec").over(w_release_date_month_artist_id).alias("release_date_month_artist_id_max_duration_sec"),
        F.nth_value("release_date", 2).over(w_all_by_duration_sec_full).alias("second_release_date"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.ntile(5).over(w_release_date_month_artist_id_3).alias("release_date_quintile"),
        F.ntile(3).over(w_artist_id_by_duration_sec).alias("duration_sec_tercile"),
        F.first("duration_sec", ignorenulls=True).over(w_release_date_year_artist_id_3).alias("first_known_duration_sec"),
    )
)

per_group = (
    windowed
    .groupBy(
        "release_date_quintile",
        F.date_trunc("month", "second_release_date").alias("second_release_date_month"),
        "duration_sec_tercile",
    )
    .agg(
        F.round(F.avg("artist_id_min_duration_sec"), 2).alias("avg_artist_id_min_duration_sec"),
        F.percentile_approx("release_date_quintile", 0.5).alias("median_release_date_quintile"),
        F.max("duration_sec_sum_3").alias("max_duration_sec_sum_3"),
        F.percentile_approx("release_date_month_artist_id_max_duration_sec", 0.5).alias("median_release_date_month_artist_id_max_duration"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("release_date").alias("last_release_date"),
    )
)

result = (
    per_group
    .filter(F.col("median_release_date_quintile") >= 2)
    .select(
        "release_date_quintile",
        "second_release_date_month",
        "duration_sec_tercile",
        "avg_artist_id_min_duration_sec",
        "median_release_date_quintile",
        "median_release_date_month_artist_id_max_duration",
        "last_release_date",
        (F.col("max_duration_sec_sum_3") - F.col("distinct_ids")).alias("max_duration_sec_sum_3_minus_distinct_ids"),
        F.round(F.col("release_date_quintile") / F.nullif(F.col("max_duration_sec_sum_3"), F.lit(0)), 3).alias("release_date_quintile_per_max_duration_sec_sum_3"),
    )
    .orderBy(
        F.col("median_release_date_quintile").desc(),
        F.col("duration_sec_tercile").asc_nulls_last(),
        "release_date_quintile",
        "second_release_date_month",
    )
    .limit(20)
)
