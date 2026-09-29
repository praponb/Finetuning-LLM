from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (F.col("student_id") >= 9))
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

rollup_base = (
    recent_enrollments
    .groupBy("student_id")
    .agg(F.sum("score").alias("total_score"), F.count("*").alias("cnt"))
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    rollup_base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .select(
        "student_id",
        "cnt",
        "total_enrollment_score",
        "avg_enrollment_score",
        "enrollment_count",
        F.abs(F.col("total_score") - 330.0).alias("total_score_distance"),
    )
)
