from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("score") > 8.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj_2"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

grouped = (
    recent
    .groupBy("grade_status", "student_id", "course_id")
    .agg(
        F.percentile_approx("score_adj", 0.5).alias("median_score_adj"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
)

with_children_enrollments = (
    grouped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.grade_status",
        "s.student_id",
        "s.course_id",
        "s.last_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

filtered = (
    with_children_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.enrollment_course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-31 15:00:00").cast("timestamp")), "left_anti")
)

joined_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .select(
        "s.grade_status",
        "s.student_id",
        "s.course_id",
        "s.last_enrolled_at",
        "s.enrollment_id",
        "s.enrollment_score",
        "s.enrollment_grade_status",
        "s.enrollment_course_id",
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
        F.col("e.score").alias("enrollment_score_2"),
    )
)

with_parent_enrollment = (
    joined_course.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .select(
        "s.*",
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score_3"),
        F.col("e.term").alias("enrollment_term"),
    )
)

qualified = (
    with_parent_enrollment.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")), "left_semi")
)

result = (
    qualified
    .select(
        "grade_status",
        "student_id",
        F.col("enrollment_score_2").alias("enrollment_score_2_v"),
        F.col("enrollment_score").alias("enrollment_score_v"),
    )
    .unpivot(
        ids=["grade_status", "student_id"],
        values=["enrollment_score_2_v", "enrollment_score_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("grade_status", "student_id", "metric_name")
)
