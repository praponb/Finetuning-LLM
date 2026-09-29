from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_enrolled_at_prior_2 = Window.orderBy("enrolled_at", "id").rowsBetween(-2, -1)
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_all_by_score_prior_2 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-2, -1)
w_term = Window.partitionBy("term")
w_all_by_enrolled_at_running = Window.orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_next_enrolled_at_year_by_row = Window.partitionBy(F.year("next_enrolled_at")).orderBy(F.col("rows_in_course_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_next_enrolled_at_month = Window.partitionBy(F.month("next_enrolled_at"))
w_student_id_by_course_id_avg = Window.partitionBy("student_id").orderBy("course_id_avg_score", "id").rowsBetween(-2, Window.currentRow)
w_student_id_by_course_id_avg_2 = Window.partitionBy("student_id").orderBy("course_id_avg_score", "id")
w_next_enrolled_at_year_by_row_2 = Window.partitionBy(F.year("next_enrolled_at")).orderBy(F.col("rows_in_course_id").desc_nulls_first())
w_distinct_ids = Window.partitionBy("distinct_ids")
w_all_by_last_term_min_score_t = Window.orderBy(F.col("last_term_min_score_to_course_id_avg_score_ratio").desc(), "distinct_course_id_in_next_enrolled_at_month", "course_id", "grade_status_or_unknown").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (~F.col("grade_status").isin("PASS", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

sequenced = (
    valid_enrollments
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.max("score").over(w_all_by_enrolled_at_prior_2).alias("score_max_2_prior"),
        F.lead("enrolled_at").over(w_all_by_score).alias("next_enrolled_at"),
        F.sum("score").over(w_all_by_score_prior_2).alias("score_sum_2_prior"),
        F.min("score").over(w_term).alias("term_min_score"),
        F.round(F.avg("score").over(w_course_id), 2).alias("course_id_avg_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.count(F.lit(1)).over(w_all_by_enrolled_at_running).alias("running_count"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.last("term_min_score").over(w_next_enrolled_at_year_by_row).alias("last_term_min_score"),
        F.size(F.collect_set("course_id").over(w_next_enrolled_at_month)).alias("distinct_course_id_in_next_enrolled_at_month"),
        F.round(F.avg("course_id_avg_score").over(w_next_enrolled_at_month), 2).alias("next_enrolled_at_month_avg_course_id_avg_score"),
        F.round(F.avg("score_sum_2_prior").over(w_student_id_by_course_id_avg), 2).alias("score_sum_2_prior_avg_3"),
        F.ntile(10).over(w_student_id_by_course_id_avg_2).alias("course_id_avg_score_decile"),
        F.round(F.percent_rank().over(w_next_enrolled_at_year_by_row_2), 3).alias("rows_in_course_id_percent_rank"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("grade_status") == "PASS")
    .groupBy(
        "distinct_course_id_in_next_enrolled_at_month",
        "course_id",
        "grade_status_or_unknown",
    )
    .agg(
        F.round(F.sum("last_term_min_score") / F.nullif(F.sum("course_id_avg_score"), F.lit(0)), 3).alias("last_term_min_score_to_course_id_avg_score_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
        F.round(F.stddev("last_term_min_score"), 2).alias("stddev_last_term_min_score"),
    )
)

with_windows = (
    rollup_base
    .withColumn("distinct_course_id_in_distinct_ids", F.size(F.collect_set("course_id").over(w_distinct_ids)))
    .withColumn("first_stddev_last_term_min_score", F.first("stddev_last_term_min_score").over(w_all_by_last_term_min_score_t))
)

result = (
    with_windows
    .select(
        "distinct_course_id_in_next_enrolled_at_month",
        "course_id",
        "grade_status_or_unknown",
        "last_term_min_score_to_course_id_avg_score_ratio",
        "stddev_last_term_min_score",
        "distinct_course_id_in_distinct_ids",
        "first_stddev_last_term_min_score",
    )
    .orderBy(
        F.col("last_term_min_score_to_course_id_avg_score_ratio").asc(),
        F.col("distinct_course_id_in_distinct_ids").desc(),
    )
)
