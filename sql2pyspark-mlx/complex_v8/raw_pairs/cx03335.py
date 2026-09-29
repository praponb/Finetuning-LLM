from pyspark.sql import functions as F

cleaned = (
    df_students
    .filter((F.col("enrolled_on") < F.lit("2025-03-15").cast("date")) & (F.length(F.trim(F.col("student_name"))) >= 8))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "gpa",
        F.date_trunc("week", F.col("enrolled_on")).alias("enrolled_on_week"),
        F.when(F.col("cohort").isin("C2022", "C2021"), "group_a").when(F.col("cohort") == "C2023", "group_b").otherwise("other").alias("cohort_group"),
        F.upper(F.trim(F.col("major"))).alias("major_clean"),
    )
)

line_level_enrollments = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.major",
        "s.gpa",
        "s.cohort_group",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    line_level_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    with_counts
    .rollup("enrollment_grade_status", "enrollment_course_id", "enrollment_term")
    .agg(
        F.round(F.avg("gpa"), 2).alias("avg_gpa"),
        F.array_sort(F.collect_set("major")).alias("major_list"),
    )
    .orderBy(
        F.col("enrollment_grade_status").asc_nulls_last(),
        F.col("enrollment_course_id").asc_nulls_last(),
        F.col("enrollment_term").asc_nulls_last(),
    )
)
