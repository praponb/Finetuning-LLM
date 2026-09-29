from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(1, 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

summarized_rows = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

without_matches = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("score").alias("overall_min_score")))
    .filter(F.col("score") > F.col("overall_min_score"))
    .drop("overall_min_score")
)

result = (
    without_matches
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrollment_count",
    )
)
