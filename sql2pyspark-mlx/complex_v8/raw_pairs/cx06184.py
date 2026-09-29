from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select("grade_status", F.col("enrolled_at").alias("happened_at"))
)

enrollment_rows_2 = (
    df_enrollments
    .filter((F.col("student_id") <= 8) & (F.col("enrolled_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")))
    .select("term", "enrolled_at")
)

enrollment_rows_3 = (
    df_enrollments
    .filter((F.col("grade_status").isin("FAIL", "PASS")) & (F.col("student_id") <= 2))
    .select("grade_status", "enrolled_at")
)

result = (
    enrollment_rows
    .union(enrollment_rows_2)
    .union(enrollment_rows_3)
    .orderBy("happened_at", "grade_status")
)
