from pyspark.sql import functions as F

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.when(F.col("score") >= 57.0, "HIGH").when(F.col("score") >= 30.0, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("C%")), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["term", "student_id"], ["term"], ["student_id"], []],
        "term", "student_id",
    )
    .agg(F.max("enrolled_at").alias("last_enrolled_at"))
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "student_id",
        "last_enrolled_at",
    )
)
