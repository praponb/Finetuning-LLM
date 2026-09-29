from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("score") > 10.1) & (F.col("grade_status") == "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

labeled = (
    scoped_enrollments
    .filter(F.col("enrolled_at_week") < F.lit("2025-02-01 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "enrolled_at_year",
        "grade_status_clean",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.max("enrolled_at_year").alias("overall_max_enrolled_at_year")))
    .filter(F.col("enrolled_at_year") < F.col("overall_max_enrolled_at_year"))
    .drop("overall_max_enrolled_at_year")
)

result = (
    filtered
    .rollup("grade_status_or_unknown", "grade_status")
    .agg(
        F.grouping("grade_status_or_unknown").alias("is_total_grade_status_or_unknown"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
    .orderBy(
        F.col("grade_status_or_unknown").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
