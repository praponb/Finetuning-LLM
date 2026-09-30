from pyspark.sql import functions as F

base_courses = (
    df_courses
    .filter((F.col("course_code").isNotNull()) | (F.col("dept") != "Bio"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    base_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

per_group = (
    profiled
    .groupBy("credits")
    .agg(
        F.countDistinct("dept").alias("distinct_depts"),
        F.sum("min_enrollment_score").alias("total_min_enrollment_score"),
        F.count("*").alias("cnt"),
        F.round(F.sum("enrollment_count") / F.nullif(F.sum("credits"), F.lit(0)), 3).alias("enrollment_count_to_credits_ratio"),
    )
)

result = (
    per_group
    .select(
        "credits",
        F.col("total_min_enrollment_score").alias("total_min_enrollment_score_v"),
        F.col("enrollment_count_to_credits_ratio").alias("m_enrollment_count_to_credits_ratio"),
    )
    .unpivot(
        ids=["credits"],
        values=["total_min_enrollment_score_v", "m_enrollment_count_to_credits_ratio"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("credits", "metric_name")
)
