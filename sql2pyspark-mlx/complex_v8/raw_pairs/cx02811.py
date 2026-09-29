from pyspark.sql import functions as F

scoped_courses = (
    df_courses
    .filter((F.col("level") == "advanced") | (F.col("credits") > 2))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.abs(F.col("credits") - 2).alias("credits_distance"),
        F.when(F.col("level").isin("advanced", "intro"), "group_a").when(F.col("level") == "core", "group_b").otherwise("other").alias("level_group"),
        F.abs(F.col("credits") - 3).alias("credits_distance_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.count(F.when(F.col("term") == "2025T1", 1)).alias("enrollment_term_count"),
    )
)

summarized_rows = (
    scoped_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

qualified = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("credits_distance").alias("overall_min_credits_distance")))
    .filter(F.col("credits_distance") > F.col("overall_min_credits_distance"))
    .drop("overall_min_credits_distance")
)

screened = (
    qualified
    .crossJoin(qualified.agg(F.min("credits_distance_2").alias("overall_min_credits_distance_2")))
    .filter(F.col("credits_distance_2") > F.col("overall_min_credits_distance_2"))
    .drop("overall_min_credits_distance_2")
)

totals = (
    screened
    .groupBy("credits_distance", "credits")
    .agg(
        F.round(F.sum("enrollment_term_count") / F.nullif(F.sum("credits"), F.lit(0)), 3).alias("enrollment_term_count_to_credits_ratio"),
        F.min("max_enrollment_score").alias("min_max_enrollment_score"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("level") == "core", F.col("enrollment_term_count"))).alias("enrollment_term_count_level_core"),
        F.sum(F.when(F.col("dept") == "Art", 1).otherwise(0)).alias("n_dept_art"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

scored = (
    totals
    .filter(F.col("enrollment_term_count_to_credits_ratio") < 9.0)
    .select(
        "*",
        F.round(F.col("min_max_enrollment_score") * 1.5, 2).alias("min_max_enrollment_score_adj"),
    )
)

result = (
    scored
    .select(
        "credits_distance",
        "credits",
        "row_count",
        "distinct_ids",
        "min_max_enrollment_score_adj",
    )
    .orderBy(F.col("credits").desc(), "credits_distance")
    .limit(20)
)
