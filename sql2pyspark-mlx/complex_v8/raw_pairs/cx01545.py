from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "PASS")) & (F.col("term") == "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id") <= 14)
    .select(F.col("id").alias("key_id"), "enrolled_at")
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-07 12:00:00").cast("timestamp")) & (F.col("grade_status") == "WITHDRAWN"))
    .select("student_id", "enrolled_at")
)

unioned = enrollment_side.union(enrollment_rows)

result = (
    unioned
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_at").alias("latest_enrolled_at"),
    )
    .orderBy("key_id")
)
