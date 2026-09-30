from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") > 17.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.substring(F.trim(F.col("grade_status")), 1, 2).alias("grade_status_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.sum("score").alias("total_enrollment_score"))
)

with_stats = (
    recent_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.total_enrollment_score")
)

without_matches = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") < 83.1) & (F.hour(F.col("e.enrolled_at")).between(8, 14)), "left_anti")
    .filter(F.col("total_enrollment_score").between(150.0, 450.0))
)

result = (
    without_matches
    .filter(F.col("days_since_enrolled_at") >= 175)
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "grade_status_prefix",
        "days_since_enrolled_at",
    )
    .orderBy(F.col("score").asc_nulls_last())
)
