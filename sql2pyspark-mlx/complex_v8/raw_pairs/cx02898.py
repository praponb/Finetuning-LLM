from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_moving_1 = Window.orderBy("score", "id", "enrollment_id").rowsBetween(-1, Window.currentRow)
w_enrollment_term_course_id_by = Window.partitionBy("enrollment_term", "course_id").orderBy(F.col("score").asc_nulls_last(), "id", "enrollment_id").rowsBetween(-3, -1)
w_student_id_by_score_minus_me = Window.partitionBy("student_id").orderBy("score_minus_median", "id", "enrollment_id")
w_all_by_score_full = Window.orderBy("score", "id", "enrollment_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_ties = Window.orderBy("score")

base_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 10.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

grade_status_stats = (
    base_enrollments
    .groupBy("grade_status")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    base_enrollments.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

with_children = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.median_score",
        "s.score_minus_median",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_trends = (
    with_children
    .select(
        "*",
        F.sum("score_minus_median").over(w_all_by_score_moving_1).alias("score_minus_median_sum_2"),
        F.sum("score_minus_median").over(w_enrollment_term_course_id_by).alias("score_minus_median_sum_3_prior"),
        F.row_number().over(w_student_id_by_score_minus_me).alias("score_minus_median_seq"),
        F.lead("enrollment_score", 3).over(w_student_id_by_score_minus_me).alias("next3_enrollment_score"),
        F.nth_value("median_score", 2).over(w_all_by_score_full).alias("second_median_score"),
        F.round(F.cume_dist().over(w_all_by_score_ties), 3).alias("score_cume_dist"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["enrollment_id", "score_minus_median_seq", "course_id"], ["enrollment_id"], ["score_minus_median_seq"], []],
        "enrollment_id", "score_minus_median_seq", "course_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("enrollment_term") == "2025T2", 1).otherwise(0)).alias("n_enrollment_term_2025t2"),
        F.round(100 * F.count(F.when(F.col("enrollment_term") == "2025T2", 1)) / F.count("*"), 1).alias("pct_enrollment_term_2025t2"),
    )
    .orderBy(
        F.col("enrollment_id").asc_nulls_last(),
        F.col("score_minus_median_seq").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
