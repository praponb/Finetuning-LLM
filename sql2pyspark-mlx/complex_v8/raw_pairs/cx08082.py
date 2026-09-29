from pyspark.sql import functions as F

cleaned = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.lower(F.col("course_code")).like("%ar%")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

labeled = (
    cleaned
    .filter(F.col("credits") > 2)
    .select(
        "*",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.abs(F.col("credits") - 3).alias("credits_distance"),
    )
)

enriched = (
    labeled
    .filter(F.col("dept").isNotNull())
    .select(
        "*",
        F.coalesce(F.col("credits_filled"), F.lit(0)).alias("credits_filled_filled"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.dept",
        "s.level",
        "s.credits_distance",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

without_matches = (
    exploded_detail.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id").isNotNull()), "left_anti")
)

scored = (
    without_matches
    .select(
        "id",
        "course_code",
        "level",
        "credits_distance",
        "enrollment_id",
        "enrollment_term",
        F.lower(F.regexp_replace(F.trim(F.col("level")), " ", "_")).alias("level_slug"),
        F.coalesce(F.col("credits_distance"), F.lit(0)).alias("credits_distance_filled"),
    )
)

result = (
    scored
    .groupBy("enrollment_term", "enrollment_id")
    .pivot("level", ["intro", "core", "advanced"])
    .agg(F.count("credits_distance_filled").alias("count_v"), F.max("credits_distance_filled").alias("max_v"))
    .orderBy("enrollment_term", "enrollment_id")
)
