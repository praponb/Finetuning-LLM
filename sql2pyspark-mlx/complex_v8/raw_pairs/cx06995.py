from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id") >= 18) & (F.col("course_id") >= 4))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
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
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

course_id_stats = (
    scored
    .groupBy("course_id")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    scored.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

enriched = (
    above_average
    .withColumn("term_or_unknown_clean", F.lower(F.trim(F.col("term_or_unknown"))))
)

result = (
    enriched
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "term_or_unknown",
        "avg_score",
        "score_minus_avg",
        "term_or_unknown_clean",
    )
)
