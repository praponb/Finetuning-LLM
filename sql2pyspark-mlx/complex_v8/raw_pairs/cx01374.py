from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter(F.col("level") == "advanced")
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
        F.when(F.col("credits") >= 4, "large").when(F.col("credits") >= 2, "medium").otherwise("small").alias("credits_tier"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown_2"),
    )
)

annotated = (
    prepared
    .select(
        "*",
        F.lower(F.trim(F.col("level_or_unknown"))).alias("level_or_unknown_clean"),
        F.length(F.trim(F.col("level"))).alias("level_len"),
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count(F.when(F.col("grade_status") == "FAIL", 1)).alias("enrollment_grade_status_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

profiled = (
    annotated.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.first_enrollment_enrolled_at_2",
    )
)

result = (
    profiled
    .groupingSets(
        [["level_or_unknown", "credits", "level_or_unknown_2"], ["level_or_unknown"], ["credits"]],
        "level_or_unknown", "credits", "level_or_unknown_2",
    )
    .agg(
        F.grouping("level_or_unknown").alias("is_total_level_or_unknown"),
        F.sum("enrollment_grade_status_count").alias("total_enrollment_grade_status_count"),
        F.min("enrollment_grade_status_count").alias("min_enrollment_grade_status_count"),
    )
    .orderBy(
        F.col("level_or_unknown").asc_nulls_last(),
        F.col("credits").asc_nulls_last(),
        F.col("level_or_unknown_2").asc_nulls_last(),
    )
)
