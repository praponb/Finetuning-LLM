from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter(F.col("major") == "Biz")
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
    )
)

annotated = (
    valid_students
    .select(
        "*",
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
        F.when(F.col("cohort").isin("C2023", "C2022"), "group_a").when(F.col("cohort") == "C2021", "group_b").otherwise("other").alias("cohort_group"),
        F.substring(F.trim(F.col("is_international_label")), 1, 3).alias("is_international_label_prefix"),
    )
)

line_level = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.student_name",
        "s.cohort",
        "s.enrolled_on",
        "s.gpa",
        "s.is_international",
        "s.is_international_label",
        "s.cohort_group",
        "s.is_international_label_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    line_level.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled
    .groupBy("enrollment_count", "enrolled_on")
    .pivot("cohort", ["C2022", "C2023", "C2021", "C2024"])
    .agg(F.sum("gpa"))
    .orderBy("enrollment_count", "enrolled_on")
)
