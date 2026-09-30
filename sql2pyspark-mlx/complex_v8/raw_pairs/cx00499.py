from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_is_explicit_f = Window.partitionBy("is_explicit").orderBy("is_explicit_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_is_explicit_f_2 = Window.partitionBy("is_explicit").orderBy("is_explicit_flag", "id")
w_is_explicit_by_is_explicit_f_3 = Window.partitionBy("is_explicit").orderBy("is_explicit_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_flag_is_explicit = Window.partitionBy("is_explicit_flag", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_flag_is_explicit_2 = Window.partitionBy("is_explicit_flag", "is_explicit").orderBy(F.col("duration_sec").desc())
w_distinct_artist_id_in_releas = Window.partitionBy("distinct_artist_id_in_release_date_year", "n_not_is_explicit")
w_distinct_artist_id_in_releas_2 = Window.partitionBy("distinct_artist_id_in_release_date_year", "n_not_is_explicit").orderBy(F.col("n_not_is_explicit").desc(), "distinct_artist_id_in_release_date_year")
w_distinct_artist_id_in_releas_3 = Window.partitionBy("distinct_artist_id_in_release_date_year", "n_not_is_explicit").orderBy(F.col("n_not_is_explicit").desc(), "distinct_artist_id_in_release_date_year").rowsBetween(-6, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter(F.col("bpm").between(76, 156))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

ranked = (
    valid_tracks
    .select(
        "*",
        F.nth_value("release_date", 2).over(w_is_explicit_by_is_explicit_f).alias("second_release_date"),
        F.row_number().over(w_is_explicit_by_is_explicit_f_2).alias("rn"),
        F.min("duration_sec").over(w_is_explicit_by_is_explicit_f_3).alias("min_duration_sec_so_far"),
        F.row_number().over(w_is_explicit_flag_is_explicit).alias("rn_2"),
        F.nth_value("is_explicit_flag", 2).over(w_is_explicit_by_is_explicit_f).alias("second_is_explicit_flag"),
        F.lead("duration_sec").over(w_is_explicit_by_is_explicit_f_2).alias("next_duration_sec"),
        F.round(F.percent_rank().over(w_is_explicit_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.lag("duration_sec", 2, 0).over(w_is_explicit_by_duration_sec).alias("prev2_duration_sec"),
        F.sum("is_explicit_flag").over(w_release_date_year).alias("release_date_year_sum_is_explicit_flag"),
        F.ntile(3).over(w_is_explicit_by_is_explicit_f_2).alias("is_explicit_flag_tercile"),
        F.round(F.col("is_explicit_flag") - F.avg("is_explicit_flag").over(w_release_date_year), 2).alias("is_explicit_flag_vs_release_date_year_avg"),
        F.first("is_explicit_flag").over(w_is_explicit_by_duration_sec_2).alias("first_is_explicit_flag"),
        F.rank().over(w_is_explicit_flag_is_explicit_2).alias("duration_sec_rank"),
        F.round(F.avg("duration_sec").over(w_release_date_year), 2).alias("release_date_year_avg_duration_sec"),
        F.size(F.collect_set("artist_id").over(w_release_date_year)).alias("distinct_artist_id_in_release_date_year"),
    )
)

stats = (
    ranked
    .groupBy("distinct_artist_id_in_release_date_year")
    .agg(
        F.round(F.sum("distinct_artist_id_in_release_date_year") / F.nullif(F.sum("first_is_explicit_flag"), F.lit(0)), 3).alias("distinct_artist_id_in_release_date_year_to_first"),
        F.sum(F.when(~F.col("is_explicit"), 1).otherwise(0)).alias("n_not_is_explicit"),
    )
)

with_windows = (
    stats
    .select(
        "*",
        F.max("n_not_is_explicit").over(w_distinct_artist_id_in_releas).alias("distinct_artist_id_in_release_date_year_n_not_is"),
        F.ntile(4).over(w_distinct_artist_id_in_releas_2).alias("n_not_is_explicit_quartile"),
        F.sum("n_not_is_explicit").over(w_distinct_artist_id_in_releas).alias("distinct_artist_id_in_release_date_year_n_not_is_2"),
        F.max("n_not_is_explicit").over(w_distinct_artist_id_in_releas_3).alias("n_not_is_explicit_max_7"),
    )
)

result = (
    with_windows
    .select(
        "distinct_artist_id_in_release_date_year",
        "distinct_artist_id_in_release_date_year_to_first",
        "n_not_is_explicit",
        "distinct_artist_id_in_release_date_year_n_not_is",
        "distinct_artist_id_in_release_date_year_n_not_is_2",
        F.greatest(F.col("n_not_is_explicit"), F.col("n_not_is_explicit_max_7")).alias("greatest_n_not_is_explicit_n_not_is_explicit_max"),
        F.coalesce(F.col("n_not_is_explicit_max_7"), F.lit(0)).alias("n_not_is_explicit_max_7_filled"),
    )
    .orderBy(
        F.col("distinct_artist_id_in_release_date_year_n_not_is").desc(),
        "distinct_artist_id_in_release_date_year",
    )
    .limit(5)
)
