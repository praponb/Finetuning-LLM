from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_ties = Window.orderBy(F.col("release_date").desc_nulls_first())
w_all_by_days_since_release_da = Window.orderBy("days_since_release_date", "id").rowsBetween(-6, Window.currentRow)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_all_by_release_date = Window.orderBy(F.col("release_date").desc_nulls_first(), "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-2, -1)

base_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_windows = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        F.round(F.cume_dist().over(w_all_by_release_date_ties), 3).alias("release_date_cume_dist"),
        F.sum("days_since_release_date").over(w_all_by_days_since_release_da).alias("days_since_release_date_sum_7"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("duration_sec_seq"),
        (F.col("days_since_release_date") - F.lag("days_since_release_date").over(w_all_by_release_date)).alias("days_since_release_date_change"),
        F.nth_value("days_since_release_date", 2).over(w_artist_id_by_duration_sec_fu).alias("second_days_since_release_date"),
        F.max("days_since_release_date").over(w_artist_id).alias("artist_id_max_days_since_release_date"),
        F.sum("days_since_release_date").over(w_artist_id).alias("artist_id_sum_days_since_release_date"),
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.first("duration_sec").over(w_artist_id_by_duration_sec_fu).alias("first_duration_sec"),
        F.first("days_since_release_date", ignorenulls=True).over(w_is_explicit_artist_id_by_dur).alias("first_known_days_since_release_date"),
        F.round(F.avg("days_since_release_date").over(w_is_explicit_artist_id_by_dur_2), 2).alias("days_since_release_date_avg_2_prior"),
    )
)

totals = (
    with_windows
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("second_days_since_release_date").alias("min_second_days_since_release_date"),
        F.min("first_known_days_since_release_date").alias("min_first_known_days_since_release_date"),
        F.min("days_since_release_date_avg_2_prior").alias("min_days_since_release_date_avg_2_prior"),
        F.max("first_duration_sec").alias("max_first_duration_sec"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)

result = (
    totals
    .select(
        "artist_id",
        "cnt",
        "min_second_days_since_release_date",
        "max_first_duration_sec",
        "distinct_artist_ids",
        (F.col("max_first_duration_sec") - F.col("cnt")).alias("max_first_duration_sec_minus_cnt"),
        F.least(F.col("max_first_duration_sec"), F.col("distinct_artist_ids")).alias("least_max_first_duration_sec_distinct_artist_ids"),
    )
    .orderBy(F.col("cnt").desc(), F.col("distinct_artist_ids").desc(), "artist_id")
    .limit(5)
)
