from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 57.6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.when(F.col("score") >= 81.4, "large").when(F.col("score") >= 26.2, "medium").otherwise("small").alias("score_tier"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isin("2025T1", "2025T2", "2024T1"), 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    recent_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .select("id", "days_since_enrolled_at", "enrollment_count")
    .unpivot(
        ids=["id"],
        values=["days_since_enrolled_at", "enrollment_count"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
