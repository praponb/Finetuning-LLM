from pyspark.sql import functions as F

valid = (
    df_students
    .filter((F.col("major") != "Biz") & (F.upper(F.trim(F.col("student_name"))).like("B%")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

screened = (
    valid
    .crossJoin(valid.agg(F.min("gpa").alias("overall_min_gpa")))
    .filter(F.col("gpa") > F.col("overall_min_gpa"))
    .drop("overall_min_gpa")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

summarized_rows = (
    screened.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "enrollment_count",
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
    )
)
