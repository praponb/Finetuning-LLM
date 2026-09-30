from pyspark.sql import functions as F

filtered = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on_2"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.when(F.col("gpa") >= 3.2, "large").when(F.col("gpa") >= 2.7, "medium").otherwise("small").alias("gpa_tier"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.min("score").alias("min_enrollment_score"),
    )
)

with_counts = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.avg_enrollment_score", "e.min_enrollment_score")
)

grouped = (
    with_counts
    .groupBy("cohort", "major")
    .agg(
        F.percentile_approx("gpa", 0.5).alias("median_gpa"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("days_since_enrolled_on_2") / F.nullif(F.sum("days_since_enrolled_on"), F.lit(0)), 3).alias("days_since_enrolled_on_2_to_days_since_enrolled_"),
        F.countDistinct("gpa_tier").alias("distinct_gpa_tiers"),
    )
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.max("n_rows").alias("overall_max_n_rows")))
    .filter(F.col("n_rows") < F.col("overall_max_n_rows"))
    .drop("overall_max_n_rows")
)

with_activity = (
    without_matches
    .crossJoin(without_matches.agg(F.avg("distinct_gpa_tiers").alias("overall_avg_distinct_gpa_tiers")))
    .filter(F.col("distinct_gpa_tiers") > F.col("overall_avg_distinct_gpa_tiers"))
    .drop("overall_avg_distinct_gpa_tiers")
)

result = (
    with_activity
    .groupBy("major", "n_rows")
    .pivot("cohort", ["C2022", "C2024", "C2021", "C2023"])
    .agg(F.min("median_gpa").alias("min_v"), F.count("median_gpa").alias("n"))
    .orderBy("major", "n_rows")
)
