from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 11) & (F.col("term") != "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_activity = (
    prepared_enrollments
    .crossJoin(prepared_enrollments.agg(F.avg("score_filled").alias("overall_avg_score_filled")))
    .filter(F.col("score_filled") > F.col("overall_avg_score_filled"))
    .drop("overall_avg_score_filled")
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "term_ref",
        "score_filled",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)
