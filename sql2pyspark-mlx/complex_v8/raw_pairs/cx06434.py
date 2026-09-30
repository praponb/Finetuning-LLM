from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

scored = (
    valid_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("days_since_enrolled_at") / F.nullif(F.col("score"), F.lit(0)), 3).alias("days_since_enrolled_at_per_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term") != "2024T1", 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled
    .cube("enrollment_count", "student_id", "enrollment_term_count")
    .agg(
        F.sum(F.when(F.col("term") == "2025T2", 1).otherwise(0)).alias("n_term_2025t2"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.sum(F.when(F.col("grade_status").isNotNull(), F.col("enrollment_count"))).alias("enrollment_count_grade_status_is_not_null"),
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("enrollment_term_count").asc_nulls_last(),
    )
)
