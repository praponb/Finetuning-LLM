from pyspark.sql import functions as F

filtered_courses = (
    df_courses
    .filter((F.col("level") == "advanced") & (F.col("dept") != "Biz"))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

filtered = (
    filtered_courses
    .crossJoin(filtered_courses.agg(F.avg("credits").alias("overall_avg_credits")))
    .filter(F.col("credits") > F.col("overall_avg_credits"))
    .drop("overall_avg_credits")
)

filtered_2 = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status") == "INCOMPLETE") & (F.col("e.score") <= 79.3), "left_semi")
    .filter(F.col("credits").between(2, 3))
)

result = (
    filtered_2
    .rollup("course_code_len", "credits", "level")
    .agg(
        F.sum(F.when(F.col("level") != "advanced", 1).otherwise(0)).alias("n_level_advanced"),
    )
    .select(
        "course_code_len",
        "credits",
        F.coalesce(F.col("level"), F.lit("ALL")).alias("level"),
        "n_level_advanced",
    )
    .orderBy(
        F.col("course_code_len").asc_nulls_last(),
        F.col("credits").asc_nulls_last(),
        F.col("level").asc_nulls_last(),
    )
)
