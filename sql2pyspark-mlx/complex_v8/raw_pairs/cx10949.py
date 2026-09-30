from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_by_second_duration_se = Window.partitionBy("n_rows").orderBy(F.col("second_duration_sec_to_duration_sec_decile_ratio").desc(), "artist_id").rowsBetween(-1, Window.currentRow)
w_n_rows_by_second_duration_se_2 = Window.partitionBy("n_rows").orderBy(F.col("second_duration_sec_to_duration_sec_decile_ratio").desc(), "artist_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("P%")) | (F.col("bpm").between(75, 155)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

screened = (
    recent
    .crossJoin(recent.agg(F.min("duration_sec").alias("overall_min_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_min_duration_sec"))
    .drop("overall_min_duration_sec")
)

windowed = (
    screened
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "release_date_label",
        F.ntile(10).over(w_all_by_duration_sec).alias("duration_sec_decile"),
        F.nth_value("duration_sec", 2).over(w_all_by_duration_sec_full).alias("second_duration_sec"),
        F.min("bpm").over(w_all_by_duration_sec_running).alias("min_bpm_so_far"),
    )
)

stats = (
    windowed
    .filter(F.col("duration_sec_decile") <= 9)
    .groupBy("artist_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
        F.min("second_duration_sec").alias("min_second_duration_sec"),
        F.round(F.sum("second_duration_sec") / F.nullif(F.sum("duration_sec_decile"), F.lit(0)), 3).alias("second_duration_sec_to_duration_sec_decile_ratio"),
    )
)

with_trends = (
    stats
    .select(
        "artist_id",
        "distinct_ids",
        "n_rows",
        "min_second_duration_sec",
        "second_duration_sec_to_duration_sec_decile_ratio",
        F.sum("min_second_duration_sec").over(w_n_rows_by_second_duration_se).alias("min_second_duration_sec_sum_2"),
        F.min("distinct_ids").over(w_n_rows_by_second_duration_se_2).alias("min_distinct_ids_so_far"),
    )
)

detail_rows = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "inner")
    .select(
        "s.artist_id",
        "s.distinct_ids",
        "s.n_rows",
        "s.min_second_duration_sec",
        "s.second_duration_sec_to_duration_sec_decile_ratio",
        "s.min_second_duration_sec_sum_2",
        "s.min_distinct_ids_so_far",
        F.col("t.id").alias("track_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["n_rows", "track_is_explicit"], ["n_rows"], []],
        "n_rows", "track_is_explicit",
    )
    .agg(
        F.max("second_duration_sec_to_duration_sec_decile_ratio").alias("max_second_duration_sec_to_duration_sec_decile_r"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
    )
    .orderBy(
        F.col("n_rows").asc_nulls_last(),
        F.col("track_is_explicit").asc_nulls_last(),
    )
)
