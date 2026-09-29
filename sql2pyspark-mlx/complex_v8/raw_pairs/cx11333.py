from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (~F.col("c.dept").isin("Biz", "Math")), "left_semi")
)

grade_status_stats = (
    without_matches
    .groupBy("grade_status")
    .agg(F.max("score").alias("max_score"))
)

above_average = (
    without_matches.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

qualified = (
    above_average.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2025T2") & (F.col("e.student_id").isNotNull()), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "grade_status_or_unknown",
        "max_score",
        "score_minus_max",
        F.least(F.col("score"), F.col("max_score")).alias("least_score_max_score"),
    )
    .orderBy("id", F.col("term").desc())
)
