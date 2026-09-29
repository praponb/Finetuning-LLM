from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))
w_all_by_release_date_ties = Window.orderBy("release_date")
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_is_explic_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("release_date", "id").rowsBetween(-6, -1)
w_release_date_month_is_explic_3 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("release_date", "id")
w_release_date_month_is_explic_4 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_rows_in_artist_id_rel = Window.orderBy(F.col("rows_in_artist_id_release_date_month").desc_nulls_first(), "id")
w_is_explicit_release_date_ran = Window.partitionBy("is_explicit", "release_date_rank").orderBy("days_since_prev_release_date", "id")
w_all_by_rows_in_artist_id_rel_2 = Window.orderBy(F.col("rows_in_artist_id_release_date_month").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_bpm_tier_in_artist = Window.partitionBy("distinct_bpm_tier_in_artist_id_release_date_mont")

cleaned = (
    df_tracks
    .filter((F.col("duration_sec") <= 352) & (F.lower(F.col("title")).like("%ea%")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 155, "HIGH").when(F.col("bpm") >= 96, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "bpm_tier",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_release_date_month), F.lit(0)), 4).alias("bpm_share_of_artist_id_release_date_month"),
        F.count(F.lit(1)).over(w_artist_id_release_date_month).alias("rows_in_artist_id_release_date_month"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_release_date_month), 2).alias("bpm_vs_artist_id_release_date_month_avg"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_month), 2).alias("artist_id_release_date_month_avg_bpm"),
        F.max("bpm").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_max_bpm"),
        F.round(F.cume_dist().over(w_all_by_release_date_ties), 3).alias("release_date_cume_dist"),
        F.first("bpm").over(w_release_date_month_is_explic).alias("first_bpm"),
        F.max("bpm").over(w_release_date_month_is_explic_2).alias("bpm_max_6_prior"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_release_date_month_is_explic_3)).alias("days_since_prev_release_date"),
        F.sum("bpm").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_sum_bpm"),
        F.first("bpm", ignorenulls=True).over(w_release_date_month_is_explic_4).alias("first_known_bpm"),
        F.lead("release_date").over(w_all_by_release_date).alias("next_release_date"),
        F.lead("bpm", 2).over(w_all_by_release_date).alias("next2_bpm"),
        F.row_number().over(w_all_by_release_date).alias("rn"),
        F.rank().over(w_all_by_release_date_ties).alias("release_date_rank"),
        F.size(F.collect_set("bpm_tier").over(w_artist_id_release_date_month)).alias("distinct_bpm_tier_in_artist_id_release_date_mont"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("rn") == 1) & (F.col("artist_id_release_date_month_sum_bpm") >= 582))
)

ranked = (
    first_rows
    .select(
        "*",
        F.ntile(10).over(w_all_by_rows_in_artist_id_rel).alias("rows_in_artist_id_release_date_month_decile"),
        F.lead("release_date_rank").over(w_all_by_rows_in_artist_id_rel).alias("next_release_date_rank"),
        F.row_number().over(w_is_explicit_release_date_ran).alias("seq_no"),
        F.nth_value("bpm_max_6_prior", 2).over(w_all_by_rows_in_artist_id_rel_2).alias("second_bpm_max_6_prior"),
        F.size(F.collect_set("bpm_tier").over(w_distinct_bpm_tier_in_artist)).alias("distinct_bpm_tier_in_distinct_bpm_tier_in_artist"),
        F.lead("next_release_date", 2).over(w_all_by_rows_in_artist_id_rel).alias("next2_next_release_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "title",
        "is_explicit",
        "bpm",
        "bpm_tier",
        "rows_in_artist_id_release_date_month",
        "artist_id_release_date_month_max_bpm",
        "release_date_cume_dist",
        "bpm_max_6_prior",
        "artist_id_release_date_month_sum_bpm",
        "first_known_bpm",
        "next_release_date",
        "next2_bpm",
        "rn",
        "release_date_rank",
        "distinct_bpm_tier_in_artist_id_release_date_mont",
        "next_release_date_rank",
        "seq_no",
        "distinct_bpm_tier_in_distinct_bpm_tier_in_artist",
        "next2_next_release_date",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
    .orderBy("first_known_bpm", F.col("rn").asc())
)
