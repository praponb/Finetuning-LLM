from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

with_details_course = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status_or_unknown",
        "s.days_since_enrolled_at",
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

labeled = (
    with_details_course
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "days_since_enrolled_at",
        "course_dept",
        F.substring(F.trim(F.col("course_dept")), 1, 3).alias("course_dept_prefix"),
        F.when(F.col("score") >= 73.0, "A").when(F.col("score") >= 34.2, "B").otherwise("C").alias("score_tier"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

result = (
    labeled
    .groupBy("student_id")
    .pivot("score_tier", ["A", "B"])
    .agg(F.avg("days_since_enrolled_at"))
    .orderBy("student_id")
)
