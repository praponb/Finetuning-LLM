from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("score") >= 38.3) & (F.col("term") == "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

term_stats = (
    filtered
    .groupBy("term")
    .agg(F.avg("score").alias("avg_score"))
)

compared = (
    filtered.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_counts = (
    compared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "term_ref",
        "enrolled_at_label",
        "avg_score",
        "score_minus_avg",
        "enrollment_count",
        F.abs(F.col("score_minus_avg") - -10.0).alias("score_minus_avg_distance"),
        (F.col("score_minus_avg") + F.col("avg_score")).alias("score_minus_avg_plus_avg_score"),
    )
)
