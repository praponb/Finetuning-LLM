from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 54.2).alias("score_distance"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.abs(F.col("score") - 63.9).alias("score_distance_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    filtered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_distance",
        "score_distance_2",
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
    .orderBy("enrolled_at_year", F.col("score_distance").desc())
)
