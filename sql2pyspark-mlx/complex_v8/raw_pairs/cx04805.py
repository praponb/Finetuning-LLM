from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("enrolled_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 57.6, "HIGH").when(F.col("score") >= 26.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

rollup_base = (
    cleaned_enrollments
    .filter(F.col("score").between(32.7, 83.1))
    .groupBy("course_id")
    .agg(
        F.round(F.stddev("enrolled_at_year"), 2).alias("stddev_enrolled_at_year"),
        F.countDistinct("score_tier").alias("distinct_score_tiers"),
        F.round(F.sum("score_filled") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("score_filled_to_score_ratio"),
    )
)

scored = (
    rollup_base
    .select(
        "course_id",
        "distinct_score_tiers",
        "score_filled_to_score_ratio",
        F.coalesce(F.col("stddev_enrolled_at_year"), F.lit(0.0)).alias("stddev_enrolled_at_year_filled"),
        F.round(F.col("distinct_score_tiers") / F.nullif(F.col("stddev_enrolled_at_year"), F.lit(0)), 3).alias("distinct_score_tiers_per_stddev_enrolled_at_year"),
        F.abs(F.col("stddev_enrolled_at_year") - 810.4).alias("stddev_enrolled_at_year_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

profiled = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at", "e.last_enrollment_enrolled_at")
)

enriched = (
    profiled.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.score_filled_to_score_ratio",
        "s.stddev_enrolled_at_year_filled",
        "s.distinct_score_tiers_per_stddev_enrolled_at_year",
        "s.first_enrollment_enrolled_at",
        "s.last_enrollment_enrolled_at",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    enriched
    .select(
        "course_id",
        "score_filled_to_score_ratio",
        "stddev_enrolled_at_year_filled",
        "distinct_score_tiers_per_stddev_enrolled_at_year",
        "course_credits",
    )
    .orderBy(
        F.col("course_id").asc(),
        F.col("stddev_enrolled_at_year_filled").desc(),
    )
    .limit(5)
)
