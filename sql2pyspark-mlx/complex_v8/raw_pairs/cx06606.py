from pyspark.sql import functions as F

filtered_courses = (
    df_courses
    .filter((~F.col("level").isin("advanced", "intro")) & (F.col("credits") > 2))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.concat(F.col("level"), F.lit("#"), F.col("id").cast("string")).alias("level_ref"),
        F.substring(F.trim(F.col("level")), 1, 3).alias("level_prefix"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

labeled = (
    filtered_courses
    .select(
        "id",
        "course_code",
        "credits",
        "level_ref",
        "level_prefix",
        (F.col("credits") - F.col("credits_distance")).alias("credits_minus_credits_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count_2"),
    )
)

profiled = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    profiled
    .rollup("enrollment_count", "credits")
    .agg(F.sum("credits").alias("total_credits"))
)
