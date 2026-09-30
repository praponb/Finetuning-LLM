from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_enrolled_at_mon = Window.partitionBy("grade_status", F.date_trunc("month", "enrolled_at"))
w_all_by_enrolled_at_ties = Window.orderBy("enrolled_at")
w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_all_by_score_filled_full = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_filled_prior_2 = Window.orderBy(F.col("score_filled").desc(), "id").rowsBetween(-2, -1)
w_all_by_enrolled_at_full = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_filled = Window.orderBy(F.col("score_filled").desc(), "id")
w_all_by_score_distance_prior = Window.orderBy("score_distance", "id").rowsBetween(-6, -1)
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")

valid = (
    df_enrollments
    .filter(F.col("score") > 43.5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.abs(F.col("score") - 69.6).alias("score_distance"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.min("score_filled").over(w_grade_status_enrolled_at_mon).alias("grade_status_enrolled_at_month_min_score_filled"),
        F.round(F.col("score_distance") / F.nullif(F.sum("score_distance").over(w_grade_status_enrolled_at_mon), F.lit(0)), 4).alias("score_distance_share_of_grade_status_enrolled_at"),
        F.round(F.cume_dist().over(w_all_by_enrolled_at_ties), 3).alias("enrolled_at_cume_dist"),
        F.count(F.lit(1)).over(w_enrolled_at_month).alias("rows_in_enrolled_at_month"),
        F.first("enrolled_at").over(w_all_by_score_filled_full).alias("first_enrolled_at"),
        F.max("score").over(w_all_by_score_filled_prior_2).alias("score_max_2_prior"),
        F.last("enrolled_at").over(w_all_by_enrolled_at_full).alias("last_enrolled_at"),
        F.round(F.cume_dist().over(w_all_by_score_filled), 3).alias("score_filled_cume_dist"),
        F.sum("score").over(w_all_by_score_distance_prior).alias("score_sum_6_prior"),
        F.row_number().over(w_all_by_enrolled_at).alias("enrolled_at_seq"),
        F.count(F.lit(1)).over(w_grade_status_enrolled_at_mon).alias("rows_in_grade_status_enrolled_at_month"),
    )
)

grouped = (
    ranked
    .filter(F.col("score_distance") < 75.0)
    .groupBy("student_id")
    .agg(
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.max("enrolled_at").alias("last_enrolled_at_2"),
        F.percentile_approx("score_distance_share_of_grade_status_enrolled_at", 0.5).alias("median_score_distance_share_of_grade_status_enro"),
        F.count("*").alias("row_count"),
        F.percentile_approx("score_distance", 0.5).alias("median_score_distance"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    grouped
    .select(
        "student_id",
        "distinct_student_ids",
        "last_enrolled_at_2",
        "median_score_distance_share_of_grade_status_enro",
        F.abs(F.col("median_score_distance") - 55.0).alias("median_score_distance_distance"),
        F.round(F.col("median_score_distance") * 1.1, 2).alias("median_score_distance_adj"),
    )
    .orderBy("median_score_distance_share_of_grade_status_enro")
)
