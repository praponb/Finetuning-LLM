from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "PASS")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

derived = (
    base
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "score_filled",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label_2"),
    )
)

without_matches = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") <= 57.6) & (F.col("e.term") == "2025T1"), "left_anti")
)

enriched = (
    without_matches
    .select(
        "*",
        F.abs(F.col("days_since_enrolled_at") - 315).alias("days_since_enrolled_at_distance"),
    )
)

result = (
    enriched
    .cube("course_id", "student_id")
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.round(F.avg("days_since_enrolled_at_distance"), 2).alias("avg_days_since_enrolled_at_distance"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
