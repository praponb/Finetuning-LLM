from pyspark.sql import functions as F

recent_courses = (
    df_courses
    .filter((F.col("credits") > 2) & (F.upper(F.trim(F.col("course_code"))).like("C%")))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.when(F.col("level").isin("core", "intro"), "group_a").when(F.col("level") == "advanced", "group_b").otherwise("other").alias("level_group"),
        F.when(F.col("credits") >= 3, "HIGH").when(F.col("credits") >= 2, "MEDIUM").otherwise("LOW").alias("credits_tier"),
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)).alias("enrollment_grade_status_count"),
    )
)

summarized_rows = (
    recent_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

annotated = (
    summarized_rows
    .select(
        "*",
        F.concat(F.col("course_code"), F.lit("#"), F.col("enrollment_count").cast("string")).alias("course_code_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
)

result = (
    annotated
    .select(
        "id",
        "course_code",
        "credits",
        "level_group",
        "credits_tier",
        "enrollment_count",
        "enrollment_count_2",
        "enrollment_grade_status_count",
        "course_code_slug",
    )
    .orderBy("course_code_slug")
)
