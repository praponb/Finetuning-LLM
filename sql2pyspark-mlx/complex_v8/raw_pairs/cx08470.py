from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("grade_status") == "PASS", 1)).alias("enrollment_grade_status_count"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

screened = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.grade_status") != "INCOMPLETE"), "left_semi")
)

qualified = (
    screened.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2025T1"), "left_anti")
)

result = (
    qualified
    .groupingSets(
        [["course_id", "grade_status"], ["course_id"], ["grade_status"]],
        "course_id", "grade_status",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "row_count",
        "first_enrolled_at",
        "distinct_student_ids",
    )
)
