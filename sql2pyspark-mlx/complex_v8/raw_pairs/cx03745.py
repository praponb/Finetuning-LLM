from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") <= 71.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)

screened = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("score").alias("overall_avg_score")))
    .filter(F.col("score") > F.col("overall_avg_score"))
    .drop("overall_avg_score")
)

result = (
    screened
    .groupBy("student_id", "enrolled_at_date")
    .pivot("grade_status", ["FAIL", "INCOMPLETE"])
    .agg(F.sum("score"))
    .orderBy("student_id", "enrolled_at_date")
)
