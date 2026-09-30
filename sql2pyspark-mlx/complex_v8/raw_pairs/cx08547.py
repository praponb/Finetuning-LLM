from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.col("course_id") >= 8) & (F.col("grade_status").isin("INCOMPLETE", "PASS", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_activity = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "left_semi")
)

screened = (
    with_activity
    .crossJoin(with_activity.agg(F.max("enrollment_count").alias("overall_max_enrollment_count")))
    .filter(F.col("enrollment_count") < F.col("overall_max_enrollment_count"))
    .drop("overall_max_enrollment_count")
)

with_children_enrollments = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") == "INCOMPLETE"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at_hour",
        "s.enrollment_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    with_children_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "enrollment_count",
        "enrollment_enrolled_at",
        "enrollment_grade_status",
    )
)
