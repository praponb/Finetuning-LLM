from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_month = Window.partitionBy("artist_id", "release_date_month").orderBy("duration_sec", "id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_prior_2 = Window.orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", "release_date_month").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month_3 = Window.partitionBy("artist_id", "release_date_month")
w_artist_id_is_explicit_by_rel_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date_month", "id")
w_all_by_duration_sec_prior_6 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_release_date_month_tercile_d = Window.partitionBy("release_date_month_tercile", "duration_sec_quartile")
w_distinct_artist_id_in_is_exp = Window.partitionBy("distinct_artist_id_in_is_explicit").orderBy("duration_sec_sum_6_prior", "id")
w_distinct_artist_id_in_is_exp_2 = Window.partitionBy("distinct_artist_id_in_is_explicit").orderBy("duration_sec_sum_6_prior")
w_all_by_seq_no = Window.orderBy("seq_no", "id")
w_distinct_artist_id_in_is_exp_3 = Window.partitionBy("distinct_artist_id_in_is_explicit").orderBy("release_date_month_share_of_artist_id_release_da", "id")

prepared_tracks = (
    df_tracks
    .filter((F.col("duration_sec") >= 184) & (F.col("bpm").between(75, 170)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.month(F.col("release_date")).alias("release_date_month"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
    )
)

with_trends = (
    prepared_tracks
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.lag("duration_sec", 1, 0).over(w_all_by_duration_sec).alias("prev_duration_sec"),
        F.first("bpm", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_bpm"),
        F.ntile(4).over(w_artist_id_release_date_month).alias("duration_sec_quartile"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.last("bpm").over(w_artist_id_is_explicit_by_rel).alias("last_bpm"),
        F.round(F.avg("release_date_month").over(w_all_by_duration_sec_prior_2), 2).alias("release_date_month_avg_2_prior"),
        F.first("duration_sec").over(w_artist_id_release_date_month_2).alias("first_duration_sec"),
        F.round(F.col("release_date_month") / F.nullif(F.sum("release_date_month").over(w_artist_id_release_date_month_3), F.lit(0)), 4).alias("release_date_month_share_of_artist_id_release_da"),
        F.row_number().over(w_artist_id_release_date_month).alias("seq_no"),
        F.round(F.percent_rank().over(w_artist_id_is_explicit_by_rel_2), 3).alias("release_date_month_percent_rank"),
        (F.col("release_date_month") - F.lag("release_date_month").over(w_artist_id_is_explicit_by_rel_2)).alias("release_date_month_change"),
        F.ntile(3).over(w_artist_id_is_explicit_by_rel_2).alias("release_date_month_tercile"),
        F.round(F.avg("duration_sec").over(w_is_explicit), 2).alias("is_explicit_avg_duration_sec"),
        F.sum("duration_sec").over(w_all_by_duration_sec_prior_6).alias("duration_sec_sum_6_prior"),
    )
)

first_rows = with_trends.filter(F.col("seq_no") <= 2)

windowed = (
    first_rows
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_release_date_month_tercile_d)).alias("distinct_artist_id_in_release_date_month_tercile"),
        F.lead("bpm").over(w_distinct_artist_id_in_is_exp).alias("next_bpm"),
        F.dense_rank().over(w_distinct_artist_id_in_is_exp_2).alias("duration_sec_sum_6_prior_dense_rank"),
        (F.col("first_duration_sec") - F.lag("first_duration_sec").over(w_distinct_artist_id_in_is_exp)).alias("first_duration_sec_change"),
        F.ntile(3).over(w_all_by_seq_no).alias("seq_no_tercile"),
        F.count(F.lit(1)).over(w_release_date_month_tercile_d).alias("rows_in_release_date_month_tercile_duration_sec_"),
        F.lead("duration_sec").over(w_distinct_artist_id_in_is_exp_3).alias("next_duration_sec"),
    )
)

result = (
    windowed
    .filter(F.col("first_known_bpm") > 89)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "release_date_month",
        "distinct_artist_id_in_is_explicit",
        "prev_duration_sec",
        "duration_sec_quartile",
        "duration_sec_percent_rank",
        "last_bpm",
        "release_date_month_avg_2_prior",
        "first_duration_sec",
        "release_date_month_share_of_artist_id_release_da",
        "seq_no",
        "release_date_month_percent_rank",
        "is_explicit_avg_duration_sec",
        "duration_sec_sum_6_prior",
        "distinct_artist_id_in_release_date_month_tercile",
        "duration_sec_sum_6_prior_dense_rank",
        "first_duration_sec_change",
        "seq_no_tercile",
        "rows_in_release_date_month_tercile_duration_sec_",
        "next_duration_sec",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.upper(F.trim(F.col("title_ref"))).alias("title_ref_clean"),
    )
    .orderBy(F.col("release_date_month_avg_2_prior").desc())
)
