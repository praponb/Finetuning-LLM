from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) | (F.col("score") > 5.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count(F.when(F.col("grade_status").isin("PASS", "INCOMPLETE", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

profiled = (
    prepared_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.min_enrollment_score",
    )
)

labeled = (
    profiled
    .select(
        "*",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.greatest(F.col("avg_enrollment_score"), F.col("enrolled_at_quarter")).alias("greatest_avg_enrollment_score_enrolled_at_quarte"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

result = (
    labeled
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_hour",
        "enrolled_at_quarter",
        "avg_enrollment_score",
        "enrollment_grade_status_count",
        "min_enrollment_score",
        "enrolled_at_year",
        "greatest_avg_enrollment_score_enrolled_at_quarte",
    )
    .orderBy("score", F.col("enrollment_grade_status_count").desc_nulls_first(), "id")
    .limit(5)
)
