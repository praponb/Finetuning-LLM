from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") > 32.7) & (F.hour(F.col("enrolled_at")).between(7, 13)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T2", "2025T1"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.when(F.col("score") >= 80.2, "HIGH").when(F.col("score") >= 36.6, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

tagged = (
    valid_enrollments
    .filter(F.col("student_id") <= 6)
    .withColumn("enrolled_at_label", F.date_format(F.col("enrolled_at"), "yyyy-MM"))
)

enriched = (
    tagged
    .select(
        "*",
        F.when(F.col("grade_status_or_unknown").isin("INCOMPLETE", "WITHDRAWN"), "group_a").when(F.col("grade_status_or_unknown") == "PASS", "group_b").otherwise("other").alias("grade_status_or_unknown_group"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.count(F.when(F.col("grade_status").isin("FAIL", "PASS", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        F.col("enrollment_grade_status_count").alias("m_enrollment_grade_status_count"),
        F.col("enrollment_count").alias("m_enrollment_count"),
    )
    .unpivot(
        ids=["id"],
        values=["m_enrollment_grade_status_count", "m_enrollment_count"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
