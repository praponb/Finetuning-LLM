from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level_or_unknown = Window.partitionBy("level_or_unknown")
w_level_or_unknown_by_credits = Window.partitionBy("level_or_unknown").orderBy("credits", "id")

filtered = (
    df_courses
    .filter((F.col("credits") < 3) & (F.col("dept") == "Math"))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.when(F.col("credits") >= 3, "HIGH").when(F.col("credits") >= 2, "MEDIUM").otherwise("LOW").alias("credits_tier"),
        F.substring(F.trim(F.col("dept")), 1, 4).alias("dept_prefix"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "credits_tier",
        "dept_prefix",
        "level_or_unknown",
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_level_or_unknown), F.lit(0)), 4).alias("credits_share_of_level_or_unknown"),
        F.lag("credits").over(w_level_or_unknown_by_credits).alias("prev_credits"),
        F.sum("credits").over(w_level_or_unknown).alias("level_or_unknown_sum_credits"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "credits",
        "credits_tier",
        "credits_share_of_level_or_unknown",
        "level_or_unknown_sum_credits",
        F.concat(F.col("dept_prefix"), F.lit("#"), F.col("prev_credits").cast("string")).alias("dept_prefix_ref"),
        F.when(F.col("credits") >= 3, "HIGH").when(F.col("credits") >= 2, "MEDIUM").otherwise("LOW").alias("credits_tier_2"),
        F.concat(F.col("level"), F.lit("#"), F.col("prev_credits").cast("string")).alias("level_ref"),
    )
)

result = (
    scored
    .select(
        "id",
        "credits_tier",
        "credits_share_of_level_or_unknown",
        "level_or_unknown_sum_credits",
        "credits_tier_2",
        "level_ref",
        F.abs(F.col("credits") - 3).alias("credits_distance"),
    )
    .orderBy(F.col("credits_tier_2").desc(), "id")
)
