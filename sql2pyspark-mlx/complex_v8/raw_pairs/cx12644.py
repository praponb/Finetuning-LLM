from pyspark.sql import functions as F

scoped = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("C%")) | (F.col("level").isin("core", "advanced")))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        F.initcap(F.trim(F.col("dept"))).alias("dept_clean"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.when(F.col("credits") >= 4, "HIGH").when(F.col("credits") >= 2, "MEDIUM").otherwise("LOW").alias("credits_tier"),
    )
)

tagged = (
    scoped
    .filter(F.col("credits_distance") >= 0)
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        "dept_clean",
        "credits_tier",
        F.abs(F.col("credits_distance") - 2).alias("credits_distance_distance"),
    )
)

derived = (
    tagged
    .filter(~F.col("dept").isin("Math", "Art"))
    .select(
        "id",
        "course_code",
        "dept",
        "dept_clean",
        "credits_tier",
        "credits_distance_distance",
        F.when(F.col("level").isin("advanced", "intro"), "group_a").when(F.col("level") == "core", "group_b").otherwise("other").alias("level_group"),
        F.when(F.col("credits_tier").isin("LOW", "HIGH"), "group_a").when(F.col("credits_tier") == "MEDIUM", "group_b").otherwise("other").alias("credits_tier_group"),
    )
)

result = (
    derived
    .select(
        "id",
        "course_code",
        "dept",
        "dept_clean",
        "credits_tier_group",
        F.concat_ws("|", F.col("credits_tier_group"), F.col("level_group")).alias("credits_tier_group_level_group_label"),
    )
    .orderBy(F.col("id").desc())
)
