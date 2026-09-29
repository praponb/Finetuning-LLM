from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") < 58.8) & (F.col("student_id") >= 7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len_2"),
    )
)

labeled = filtered_enrollments.withColumn("enrolled_at_month", F.month(F.col("enrolled_at")))

result = (
    labeled
    .groupBy("grade_status_len", "enrolled_at_month")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE", "PASS", "FAIL"])
    .agg(F.sum("term_len"))
)
