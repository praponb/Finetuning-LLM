from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
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
    .agg(F.count("*").alias("enrollment_count"))
)

with_counts = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "enrollment_count",
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("term").desc())
    .limit(20)
)
