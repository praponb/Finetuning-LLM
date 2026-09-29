from pyspark.sql import functions as F

filtered = df_courses.filter(F.col("level").isNotNull())

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isin("2024T2", "2024T1", "2025T2"), 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_activity = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status") != "WITHDRAWN") & (F.col("e.score") >= 16.3), "left_semi")
)

filtered_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("enrollment_term_count").alias("overall_avg_enrollment_term_count")))
    .filter(F.col("enrollment_term_count") > F.col("overall_avg_enrollment_term_count"))
    .drop("overall_avg_enrollment_term_count")
)

result = (
    filtered_2
    .select(
        "id",
        F.col("credits").alias("m_credits"),
        F.col("enrollment_count").alias("enrollment_count_v"),
        F.col("enrollment_term_count").alias("m_enrollment_term_count"),
    )
    .unpivot(
        ids=["id"],
        values=["m_credits", "enrollment_count_v", "m_enrollment_term_count"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
