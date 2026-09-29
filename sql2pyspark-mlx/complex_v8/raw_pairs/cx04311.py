from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("grade_status") != "FAIL") | (F.to_date(F.col("enrolled_at")) < F.lit("2025-03-14").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

term_stats = (
    base
    .groupBy("term")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    base.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "avg_score",
        "score_minus_avg",
    )
)
