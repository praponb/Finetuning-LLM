from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("student_id") <= 11)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_parent_student = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_prefix",
        "s.score_filled",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

totals = (
    with_parent_student
    .groupBy("student_id")
    .agg(
        F.round(F.sum("score"), 2).alias("total_score"),
        F.percentile_approx("score_filled", 0.5).alias("median_score_filled"),
        F.round(100 * F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)) / F.count("*"), 1).alias("pct_grade_status_incomplete"),
        F.min("score_filled").alias("min_score_filled"),
        F.min("score").alias("min_score"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 20.6)
    .drop("avg_score_check")
)

exploded_detail_enrollments = (
    totals.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.student_id",
        "s.pct_grade_status_incomplete",
        "s.min_score_filled",
        "s.min_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enrollment_grade_status_stats = (
    exploded_detail_enrollments
    .groupBy("enrollment_grade_status")
    .agg(F.avg("min_score").alias("avg_min_score"))
)

outliers = (
    exploded_detail_enrollments.alias("s")
    .join(enrollment_grade_status_stats.alias("g"), F.col("g.enrollment_grade_status") == F.col("s.enrollment_grade_status"), "inner")
    .filter(F.col("s.min_score") > F.col("g.avg_min_score"))
    .select(
        "s.*",
        "g.avg_min_score",
        F.round(F.col("s.min_score") - F.col("g.avg_min_score"), 2).alias("min_score_minus_avg"),
    )
)

exploded_detail = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.student_id",
        "s.min_score_filled",
        "s.min_score",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.avg_min_score",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

result = (
    exploded_detail
    .select(
        "student_id",
        "min_score",
        "enrollment_id",
        "enrollment_enrolled_at",
        "avg_min_score",
        "enrollment_id_2",
        "enrollment_term",
        "enrollment_enrolled_at_2",
        "enrollment_grade_status_2",
    )
    .orderBy(F.col("enrollment_enrolled_at").asc_nulls_last())
)
