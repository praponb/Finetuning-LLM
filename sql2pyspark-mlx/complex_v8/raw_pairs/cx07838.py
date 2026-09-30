from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-03-08 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

per_group = (
    valid
    .filter(F.col("score") <= 57.3)
    .groupBy("course_id", "grade_status", "term")
    .agg(
        F.min("score").alias("min_score"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_counts = (
    per_group.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score_2"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

summarized_rows = (
    with_counts.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        "e.avg_enrollment_score_2",
        "e.total_enrollment_score",
    )
)

line_level = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left")
    .select(
        "s.course_id",
        "s.grade_status",
        "s.term",
        "s.min_score",
        "s.avg_enrollment_score",
        "s.total_enrollment_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    line_level
    .filter(F.to_date(F.col("enrollment_enrolled_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "course_id",
        "term",
        "min_score",
        "avg_enrollment_score",
        "total_enrollment_score",
        "enrollment_id",
        "enrollment_enrolled_at",
        (F.col("enrollment_enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrollment_enrolled_at_plus_30m"),
    )
    .orderBy(F.col("course_id").desc(), F.col("term").desc())
)
