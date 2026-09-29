from pyspark.sql import functions as F

recent = (
    df_courses
    .filter((F.col("dept").isin("Math", "Biz")) & (F.lower(F.col("course_code")).like("%cs%")))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.when(F.col("dept").isin("Biz", "CS"), "group_a").when(F.col("dept") == "Math", "group_b").otherwise("other").alias("dept_group"),
    )
)

labeled = (
    recent
    .filter(F.col("dept_group").isin("other", "group_b"))
    .withColumn("dept_slug", F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")))
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("enrollment_term_count"),
    )
)

with_counts = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") != "PASS", 1)).alias("enrollment_grade_status_count"),
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

profiled = (
    with_counts.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    profiled
    .groupBy("enrollment_term_count", "enrollment_count")
    .pivot("dept_group", ["other", "group_a"])
    .agg(F.count("credits"))
    .orderBy("enrollment_term_count", "enrollment_count")
)
