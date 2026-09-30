from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("student_id") >= 5) & (F.col("score") <= 73.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

with_activity = (
    filtered_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") >= 11) & (F.col("e.enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")), "left_anti")
)

result = (
    with_activity
    .groupBy("course_id", "student_id")
    .pivot("grade_status", ["FAIL", "INCOMPLETE"])
    .agg(F.avg("score_adj").alias("avg_v"), F.count("score_adj").alias("n"))
)
