from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("score") > 6.2) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

labeled = filtered.withColumn("score_filled", F.coalesce(F.col("score"), F.lit(0.0)))

enriched = (
    labeled.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.level").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        "s.enrolled_at_day",
        "s.grade_status_prefix",
        "s.score_filled",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    enriched
    .filter(F.col("course_dept") == "Biz")
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "days_since_enrolled_at",
        "grade_status_prefix",
        "score_filled",
        "course_credits",
        "course_level",
        "course_dept",
    )
    .orderBy("score")
)
