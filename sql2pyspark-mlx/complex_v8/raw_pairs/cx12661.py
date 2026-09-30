from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_cohort_by_gpa_running = Window.partitionBy("major", "cohort").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_cohort_by_gpa_moving_3 = Window.partitionBy("cohort").orderBy(F.col("gpa").desc(), "id").rowsBetween(-3, Window.currentRow)
w_major = Window.partitionBy("major")
w_major_by_gpa_full = Window.partitionBy("major").orderBy(F.col("gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_cohort_by_gpa = Window.partitionBy("cohort").orderBy(F.col("gpa").desc(), "id")
w_all_by_gpa_prior_3 = Window.orderBy(F.col("gpa").desc(), "id").rowsBetween(-3, -1)
w_cohort_major_by_gpa = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").desc(), "id")
w_major_cohort_by_gpa = Window.partitionBy("major", "cohort").orderBy("gpa", "id")
w_cohort_major_by_gpa_ties = Window.partitionBy("cohort", "major").orderBy(F.col("gpa").desc())
w_all_by_total_gpa_quintile = Window.orderBy(F.col("total_gpa_quintile").asc_nulls_last(), "distinct_cohort_in_major", "gpa_vs_major_avg_filled_tier", "gpa_rank")
w_gpa_rank_gpa_vs_major_avg_fi = Window.partitionBy("gpa_rank", "gpa_vs_major_avg_filled_tier")

recent = (
    df_students
    .filter(F.col("gpa").between(2.1, 3.4))
    .select(
        "id",
        "cohort",
        "major",
        "gpa",
        F.concat(F.col("cohort"), F.lit("#"), F.col("id").cast("string")).alias("cohort_ref"),
        F.date_format(F.col("enrolled_on"), "yyyy-MM").alias("enrolled_on_label"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "cohort",
        "major",
        "cohort_ref",
        "enrolled_on_label",
        F.sum("gpa").over(w_major_cohort_by_gpa_running).alias("running_gpa"),
        F.max("gpa").over(w_cohort_by_gpa_moving_3).alias("gpa_max_4"),
        F.size(F.collect_set("cohort").over(w_major)).alias("distinct_cohort_in_major"),
        F.nth_value("gpa", 2).over(w_major_by_gpa_full).alias("second_gpa"),
        F.lead("gpa", 3).over(w_cohort_by_gpa).alias("next3_gpa"),
        F.sum("gpa").over(w_all_by_gpa_prior_3).alias("gpa_sum_3_prior"),
        F.round(F.col("gpa") - F.avg("gpa").over(w_major), 2).alias("gpa_vs_major_avg"),
        F.round(F.cume_dist().over(w_cohort_major_by_gpa), 3).alias("gpa_cume_dist"),
        F.lag("gpa", 2).over(w_major_cohort_by_gpa).alias("prev2_gpa"),
        F.ntile(5).over(w_major_cohort_by_gpa).alias("gpa_quintile"),
        F.row_number().over(w_cohort_major_by_gpa).alias("gpa_seq"),
        F.rank().over(w_cohort_major_by_gpa).alias("gpa_rank"),
        F.sum("gpa").over(w_major).alias("major_sum_gpa"),
        F.count(F.lit(1)).over(w_major).alias("rows_in_major"),
        F.round(F.percent_rank().over(w_cohort_major_by_gpa_ties), 3).alias("gpa_percent_rank"),
    )
)

derived = (
    metrics
    .filter(F.col("running_gpa") >= 11.1)
    .select(
        "id",
        "cohort",
        "cohort_ref",
        "enrolled_on_label",
        "running_gpa",
        "gpa_max_4",
        "distinct_cohort_in_major",
        "second_gpa",
        "next3_gpa",
        "gpa_sum_3_prior",
        "gpa_vs_major_avg",
        "gpa_cume_dist",
        "prev2_gpa",
        "gpa_quintile",
        "gpa_rank",
        "major_sum_gpa",
        F.concat(F.col("cohort_ref"), F.lit("#"), F.col("distinct_cohort_in_major").cast("string")).alias("cohort_ref_ref"),
        F.length(F.trim(F.col("cohort_ref"))).alias("cohort_ref_len"),
        F.coalesce(F.col("gpa_vs_major_avg"), F.lit(0.0)).alias("gpa_vs_major_avg_filled"),
    )
)

totals = (
    derived
    .filter(F.col("cohort").isin("C2022", "C2023", "C2024"))
    .groupBy(
        "distinct_cohort_in_major",
        F.when(F.col("gpa_vs_major_avg_filled") >= 1.2, "A").when(F.col("gpa_vs_major_avg_filled") >= -2.4, "B").otherwise("C").alias("gpa_vs_major_avg_filled_tier"),
        "gpa_rank",
    )
    .agg(
        F.array_sort(F.collect_set("cohort")).alias("cohort_list"),
        F.sum("gpa_quintile").alias("total_gpa_quintile"),
        F.count(F.when(F.col("cohort") == "C2024", 1)).alias("n_cohort_c2024"),
    )
)

ranked = (
    totals
    .select(
        "*",
        F.rank().over(w_all_by_total_gpa_quintile).alias("total_gpa_quintile_rank"),
        F.round(F.col("gpa_rank") / F.nullif(F.sum("gpa_rank").over(w_gpa_rank_gpa_vs_major_avg_fi), F.lit(0)), 4).alias("gpa_rank_share_of_gpa_rank_gpa_vs_major_avg_fill"),
        (F.col("distinct_cohort_in_major") - F.lag("distinct_cohort_in_major").over(w_all_by_total_gpa_quintile)).alias("distinct_cohort_in_major_change"),
    )
)

result = (
    ranked
    .select(
        "distinct_cohort_in_major",
        "gpa_vs_major_avg_filled_tier",
        "gpa_rank",
        "n_cohort_c2024",
        "total_gpa_quintile_rank",
        "gpa_rank_share_of_gpa_rank_gpa_vs_major_avg_fill",
        "distinct_cohort_in_major_change",
        F.size(F.col("cohort_list")).alias("cohort_list_count"),
    )
    .orderBy("gpa_vs_major_avg_filled_tier")
)
