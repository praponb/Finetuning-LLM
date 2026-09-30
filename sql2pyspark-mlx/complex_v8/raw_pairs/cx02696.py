from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len_is_explicit_by_dur = Window.partitionBy("title_len", "is_explicit").orderBy(F.col("duration_sec_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_distance = Window.orderBy("duration_sec_distance", "id")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")
w_all_by_duration_sec_distance_2 = Window.orderBy("duration_sec_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_title_len_running = Window.orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_title_len = Window.orderBy("title_len", "id")
w_title_len_is_explicit_by_dur_2 = Window.partitionBy("title_len", "is_explicit").orderBy(F.col("duration_sec_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_title_len_is_explicit_by_dur_3 = Window.partitionBy("title_len", "is_explicit").orderBy(F.col("duration_sec_distance").asc_nulls_last(), "id")
w_duration_sec_distance_quinti = Window.partitionBy("duration_sec_distance_quintile").orderBy(F.col("release_date_year_artist_id_avg_duration_sec_dis").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_duration_sec_di = Window.orderBy("first_duration_sec_distance", "id").rowsBetween(-3, Window.currentRow)
w_all_by_first_duration_sec_di_2 = Window.orderBy("first_duration_sec_distance", "id")
w_duration_sec_distance_quinti_2 = Window.partitionBy("duration_sec_distance_quintile").orderBy(F.col("release_date_year_artist_id_avg_duration_sec_dis").desc(), "id")
w_duration_sec_distance_quinti_3 = Window.partitionBy("duration_sec_distance_quintile")
w_all_by_max_artist_debut_year = Window.orderBy(F.col("max_artist_debut_year_bucket").desc())
w_all_by_max_artist_debut_year_2 = Window.orderBy(F.col("max_artist_debut_year_bucket").desc(), "duration_sec_distance_quintile")

valid = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

enriched_artist = (
    valid.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"), "a.artist_name")
)

metrics = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "duration_sec_distance",
        "artist_debut_year",
        F.first("artist_debut_year", ignorenulls=True).over(w_title_len_is_explicit_by_dur).alias("first_known_artist_debut_year"),
        F.lag("artist_debut_year").over(w_all_by_duration_sec_distance).alias("prev_artist_debut_year"),
        F.round(F.avg("duration_sec_distance").over(w_release_date_year_artist_id), 2).alias("release_date_year_artist_id_avg_duration_sec_dis"),
        F.nth_value("artist_debut_year", 2).over(w_all_by_duration_sec_distance_2).alias("second_artist_debut_year"),
        F.round(F.avg("title_len").over(w_release_date_year_artist_id), 2).alias("release_date_year_artist_id_avg_title_len"),
        F.lead("duration_sec_distance", 3).over(w_all_by_duration_sec_distance).alias("next3_duration_sec_distance"),
        F.max("artist_debut_year").over(w_release_date_year_artist_id).alias("release_date_year_artist_id_max_artist_debut_yea"),
        F.max("title_len").over(w_all_by_title_len_running).alias("max_title_len_so_far"),
        F.sum("duration_sec_distance").over(w_release_date_year_artist_id).alias("release_date_year_artist_id_sum_duration_sec_dis"),
        F.lead("title_len", 1, 0).over(w_all_by_title_len).alias("next_title_len"),
        F.first("duration_sec_distance").over(w_title_len_is_explicit_by_dur_2).alias("first_duration_sec_distance"),
        F.ntile(5).over(w_title_len_is_explicit_by_dur_3).alias("duration_sec_distance_quintile"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "moods",
        "duration_sec_distance",
        "first_known_artist_debut_year",
        "release_date_year_artist_id_avg_duration_sec_dis",
        "second_artist_debut_year",
        "release_date_year_artist_id_avg_title_len",
        "max_title_len_so_far",
        "release_date_year_artist_id_sum_duration_sec_dis",
        "first_duration_sec_distance",
        "duration_sec_distance_quintile",
        (F.floor(F.col("artist_debut_year") / 5) * 5).alias("artist_debut_year_bucket"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.min("first_duration_sec_distance").over(w_duration_sec_distance_quinti).alias("min_first_duration_sec_distance_so_far"),
        F.sum("duration_sec_distance").over(w_all_by_first_duration_sec_di).alias("duration_sec_distance_sum_4"),
        F.row_number().over(w_all_by_first_duration_sec_di_2).alias("seq_no"),
        F.lead("second_artist_debut_year", 3, 0).over(w_all_by_first_duration_sec_di_2).alias("next3_second_artist_debut_year"),
        F.lag("max_title_len_so_far").over(w_duration_sec_distance_quinti_2).alias("prev_max_title_len_so_far"),
        F.last("first_duration_sec_distance", ignorenulls=True).over(w_duration_sec_distance_quinti).alias("last_known_first_duration_sec_distance"),
        F.min("second_artist_debut_year").over(w_duration_sec_distance_quinti_3).alias("duration_sec_distance_quintile_min_second_artist"),
        F.round(F.avg("first_duration_sec_distance").over(w_duration_sec_distance_quinti_3), 2).alias("duration_sec_distance_quintile_avg_first_duratio"),
    )
)

rollup_base = (
    windowed
    .groupBy("duration_sec_distance_quintile")
    .agg(
        F.sum("artist_debut_year_bucket").alias("total_artist_debut_year_bucket"),
        F.max("artist_debut_year_bucket").alias("max_artist_debut_year_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

windowed_2 = (
    rollup_base
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_max_artist_debut_year), 3).alias("max_artist_debut_year_bucket_cume_dist"),
        F.lead("distinct_ids", 3).over(w_all_by_max_artist_debut_year_2).alias("next3_distinct_ids"),
        F.round(F.cume_dist().over(w_all_by_max_artist_debut_year_2), 3).alias("max_artist_debut_year_bucket_cume_dist_2"),
    )
)

result = (
    windowed_2
    .select(
        "duration_sec_distance_quintile",
        "total_artist_debut_year_bucket",
        "max_artist_debut_year_bucket",
        "distinct_ids",
        "max_artist_debut_year_bucket_cume_dist",
        "next3_distinct_ids",
        "max_artist_debut_year_bucket_cume_dist_2",
        (F.col("distinct_ids") + F.col("next3_distinct_ids")).alias("distinct_ids_plus_next3_distinct_ids"),
    )
    .orderBy(
        F.col("next3_distinct_ids").desc_nulls_first(),
        F.col("max_artist_debut_year_bucket_cume_dist_2").asc_nulls_last(),
    )
)
