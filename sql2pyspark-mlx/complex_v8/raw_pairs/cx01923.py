from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score") <= 73.0) & (F.col("grade_status") == "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    prepared_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    summarized_rows
    .rollup("term", "grade_status")
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.countDistinct("term").alias("distinct_terms"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "is_total_term",
        "distinct_terms",
        "distinct_ids",
        "stddev_score",
    )
)
