from pyspark.sql import functions as F

recent = (
    df_students
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.trim(F.col("student_name"))).alias("student_name_clean"),
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
        F.concat(F.col("major"), F.lit("#"), F.col("id").cast("string")).alias("major_ref"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (~F.col("e.grade_status").isin("FAIL", "WITHDRAWN")) & (F.col("e.term") != "2024T2"), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
    )
)

qualified = (
    with_stats
    .crossJoin(with_stats.agg(F.min("enrollment_count").alias("overall_min_enrollment_count")))
    .filter(F.col("enrollment_count") > F.col("overall_min_enrollment_count"))
    .drop("overall_min_enrollment_count")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count_2"))
)

summarized_rows = (
    qualified.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    summarized_rows
    .filter(F.col("gpa") > 2.1)
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "enrolled_on_month_end",
        "major_ref",
        "enrollment_count",
        "first_enrollment_enrolled_at",
        "enrollment_count_2",
        F.substring(F.trim(F.col("major")), 1, 2).alias("major_prefix"),
    )
    .orderBy("enrollment_count_2", F.col("enrolled_on").desc_nulls_first())
)
