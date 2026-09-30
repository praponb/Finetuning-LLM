from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("student_id") <= 17) & (F.col("term").isin("2025T1", "2024T1", "2025T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.when(F.col("score") >= 80.2, "A").when(F.col("score") >= 11.3, "B").otherwise("C").alias("score_tier"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.score_tier",
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.max_enrollment_score",
        "e.last_enrollment_enrolled_at",
    )
)

result = (
    summarized_rows
    .cube("grade_status", "score_tier")
    .agg(F.countDistinct("grade_status").alias("distinct_grade_status"))
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("score_tier").asc_nulls_last(),
    )
)
