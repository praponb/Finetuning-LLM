from pyspark.sql import functions as F

recent = (
    df_courses
    .filter((F.col("dept").isin("Bio", "Math", "Biz")) & (F.col("level") == "advanced"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.concat(F.col("course_code"), F.lit("#"), F.col("credits").cast("string")).alias("course_code_ref"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

with_counts = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

with_activity = (
    with_counts
    .crossJoin(with_counts.agg(F.avg("credits").alias("overall_avg_credits")))
    .filter(F.col("credits") > F.col("overall_avg_credits"))
    .drop("overall_avg_credits")
)

derived = (
    with_activity
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "course_code_ref",
        "total_enrollment_score",
        F.concat(F.col("course_code"), F.lit("#"), F.col("id").cast("string")).alias("course_code_ref_2"),
        F.when(F.col("level").isin("advanced", "intro"), "group_a").when(F.col("level") == "core", "group_b").otherwise("other").alias("level_group"),
    )
)

result = (
    derived
    .rollup("level_group", "credits")
    .agg(F.round(F.stddev("credits"), 2).alias("stddev_credits"))
    .orderBy(
        F.col("level_group").asc_nulls_last(),
        F.col("credits").asc_nulls_last(),
    )
)
