from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_all_by_duration_sec_bucket = Window.orderBy("duration_sec_bucket", "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm_full = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_bucket_p = Window.orderBy("duration_sec_bucket", "id").rowsBetween(-1, -1)
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec_bucket", "id")
w_artist_id_is_explicit_by_dur_3 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm_moving_3 = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(-3, Window.currentRow)
w_distinct_artist_id_in_releas = Window.partitionBy("distinct_artist_id_in_release_date_year", "bpm_quintile")
w_distinct_artist_id_in_releas_2 = Window.partitionBy("distinct_artist_id_in_release_date_year").orderBy(F.col("rows_in_release_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_bpm_2 = Window.orderBy("second_bpm_2", "id")
w_distinct_artist_id_in_releas_3 = Window.partitionBy("distinct_artist_id_in_release_date_year").orderBy(F.col("rows_in_release_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_artist_id_in = Window.orderBy(F.col("distinct_artist_id_in_release_date_year").desc(), "id").rowsBetween(-3, -1)
w_distinct_artist_id_in_releas_4 = Window.partitionBy("distinct_artist_id_in_release_date_year").orderBy(F.col("rows_in_release_date_year").desc(), "id")

filtered = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%eo%")) | (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.ntile(5).over(w_is_explicit_by_bpm).alias("bpm_quintile"),
        F.lead("bpm", 2).over(w_all_by_duration_sec_bucket).alias("next2_bpm"),
        F.count(F.lit(1)).over(w_release_date_month).alias("rows_in_release_date_month"),
        F.round(F.col("duration_sec_bucket") - F.avg("duration_sec_bucket").over(w_release_date_year), 2).alias("duration_sec_bucket_vs_release_date_year_avg"),
        F.count(F.lit(1)).over(w_release_date_year).alias("rows_in_release_date_year"),
        F.round(F.col("duration_sec_bucket") / F.nullif(F.sum("duration_sec_bucket").over(w_release_date_month), F.lit(0)), 4).alias("duration_sec_bucket_share_of_release_date_month"),
        F.first("bpm", ignorenulls=True).over(w_artist_id_is_explicit_by_dur).alias("first_known_bpm"),
        F.min("duration_sec_bucket").over(w_artist_id_is_explicit_by_rel).alias("min_duration_sec_bucket_so_far"),
        F.nth_value("bpm", 2).over(w_is_explicit_by_bpm_full).alias("second_bpm"),
        F.max("bpm").over(w_all_by_duration_sec_bucket_p).alias("bpm_max_1_prior"),
        F.size(F.collect_set("artist_id").over(w_release_date_year)).alias("distinct_artist_id_in_release_date_year"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_is_explicit_by_dur_2)).alias("bpm_change"),
        F.nth_value("bpm", 2).over(w_artist_id_is_explicit_by_dur_3).alias("second_bpm_2"),
        F.max("duration_sec_bucket").over(w_is_explicit_by_bpm_moving_3).alias("duration_sec_bucket_max_4"),
        F.lead("bpm").over(w_all_by_duration_sec_bucket).alias("next_bpm"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_distinct_artist_id_in_releas)).alias("distinct_artist_id_in_distinct_artist_id_in_rele"),
        F.last("bpm_max_1_prior", ignorenulls=True).over(w_distinct_artist_id_in_releas_2).alias("last_known_bpm_max_1_prior"),
        F.lead("release_date").over(w_all_by_second_bpm_2).alias("next_release_date"),
        F.nth_value("release_date", 2).over(w_distinct_artist_id_in_releas_3).alias("second_release_date"),
        F.max("bpm").over(w_all_by_distinct_artist_id_in).alias("bpm_max_3_prior"),
        F.round(F.cume_dist().over(w_distinct_artist_id_in_releas_4), 3).alias("rows_in_release_date_year_cume_dist"),
    )
)

rollup_base = (
    with_trends
    .groupBy("artist_id", "is_explicit")
    .agg(
        F.round(F.avg("rows_in_release_date_year_cume_dist"), 2).alias("avg_rows_in_release_date_year_cume_dist"),
        F.round(F.stddev("second_bpm_2"), 2).alias("stddev_second_bpm_2"),
        F.datediff(F.max("next_release_date"), F.min("next_release_date")).alias("next_release_date_span_days"),
    )
)

result = (
    rollup_base
    .filter(F.col("avg_rows_in_release_date_year_cume_dist") >= 0.1)
    .select(
        "artist_id",
        "is_explicit",
        "avg_rows_in_release_date_year_cume_dist",
        "stddev_second_bpm_2",
        "next_release_date_span_days",
    )
    .orderBy(F.col("stddev_second_bpm_2").desc(), "artist_id", "is_explicit")
    .limit(10)
)
