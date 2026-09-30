from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter(F.col("course_id") <= 7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count(F.when(F.col("grade_status") == "FAIL", 1)).alias("enrollment_grade_status_count"),
    )
)

with_counts = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    with_counts
    .select("id", "grade_status_len", "enrollment_count")
    .unpivot(
        ids=["id"],
        values=["grade_status_len", "enrollment_count"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
