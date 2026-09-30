from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 59.8, "A").when(F.col("score") >= 32.7, "B").otherwise("C").alias("score_tier"),
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
    )
)

course_id_stats = (
    recent_enrollments
    .groupBy("course_id")
    .agg(F.max("score").alias("max_score"))
)

outliers = (
    recent_enrollments.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") == F.col("g.max_score"))
    .select(
        "s.*",
        "g.max_score",
        F.round(F.col("s.score") - F.col("g.max_score"), 2).alias("score_minus_max"),
    )
)

result = (
    outliers
    .groupBy("enrolled_at")
    .pivot("score_tier", ["C", "B", "A"])
    .agg(F.avg("score_minus_max"))
    .orderBy("enrolled_at")
)
