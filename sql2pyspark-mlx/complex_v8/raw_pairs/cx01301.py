from pyspark.sql import functions as F

cleaned = (
    df_courses
    .filter((F.col("credits").between(2, 3)) & (F.col("level") == "core"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.abs(F.col("credits") - 2).alias("credits_distance_2"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "credits_distance_2",
        "level_or_unknown",
        F.least(F.col("credits_distance"), F.col("credits_distance_2")).alias("least_credits_distance_credits_distance_2"),
        (F.col("credits_distance") - F.col("credits")).alias("credits_distance_minus_credits"),
    )
)

derived = (
    scored
    .select(
        "*",
        F.least(F.col("credits"), F.col("credits_distance_2")).alias("least_credits_credits_distance_2"),
    )
)

result = (
    derived
    .select(
        "id",
        "course_code",
        "dept",
        "credits_distance_2",
        "level_or_unknown",
        "credits_distance_minus_credits",
        "least_credits_credits_distance_2",
    )
    .orderBy(
        F.col("credits_distance_2").desc_nulls_first(),
        F.col("least_credits_credits_distance_2").asc(),
    )
)
