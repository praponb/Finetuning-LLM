from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("student_id") <= 3) & (F.col("term").isin("2024T2", "2025T1")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

grouped = (
    base
    .groupBy("course_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.min("score").alias("min_score"),
        F.max("score").alias("max_score_check"),
    )
    .filter(F.col("max_score_check") > 16.3)
    .drop("max_score_check")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    grouped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled
    .select(
        "course_id",
        "avg_score",
        "first_enrolled_at",
        "distinct_grade_status",
        "last_enrolled_at",
        "min_score",
        "enrollment_count",
        F.coalesce(F.col("distinct_grade_status"), F.lit(0)).alias("distinct_grade_status_filled"),
    )
)
