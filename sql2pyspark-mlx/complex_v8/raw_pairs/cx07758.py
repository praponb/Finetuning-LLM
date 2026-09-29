from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("student_id") >= 13))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.when(F.col("score") >= 57.8, "HIGH").when(F.col("score") >= 13.9, "MEDIUM").otherwise("LOW").alias("score_tier"),
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_ref",
        "s.enrolled_at_plus_15m",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

result = (
    joined
    .cube("course_credits", "course_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("course_credits").alias("total_course_credits"),
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
