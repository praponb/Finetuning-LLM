from pyspark.sql import functions as F

prepared = (
    df_students
    .filter((F.col("gpa").between(2.1, 3.9)) | (F.upper(F.trim(F.col("student_name"))).like("D%")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.abs(F.col("gpa") - 2.7).alias("gpa_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count(F.when(F.col("grade_status").isin("PASS", "WITHDRAWN", "FAIL"), 1)).alias("enrollment_grade_status_count"),
    )
)

profiled = (
    prepared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

labeled = (
    profiled
    .filter(F.col("avg_enrollment_score") > 25.0)
    .select(
        "id",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "gpa_distance",
        "enrollment_grade_status_count",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on_2"),
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
    )
)

result = (
    labeled
    .filter(F.col("enrollment_grade_status_count") <= 5)
    .select(
        "id",
        "enrolled_on",
        "gpa",
        "is_international",
        "gpa_distance",
        "enrollment_grade_status_count",
        "days_since_enrolled_on_2",
        "cohort_or_unknown",
        "enrolled_on_month_end",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on_month_end")).alias("days_since_enrolled_on_month_end"),
        F.length(F.trim(F.col("cohort_or_unknown"))).alias("cohort_or_unknown_len"),
    )
    .orderBy(
        F.col("id").desc(),
        F.col("days_since_enrolled_on_2").desc_nulls_first(),
    )
)
