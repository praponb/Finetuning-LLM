from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

screened = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") >= 2) & (F.col("e.term").isin("2024T1", "2025T2")), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    screened.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "WITHDRAWN"), "left_anti")
    .filter(F.col("student_id").isNotNull())
)

result = (
    with_activity
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status_len",
        "last_enrollment_enrolled_at",
        F.round(F.col("grade_status_len") / F.nullif(F.col("score"), F.lit(0)), 3).alias("grade_status_len_per_score"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len_2"),
    )
    .orderBy("id")
)
