from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id") <= 6))
    .select(
        F.lit("enrollment").alias("record_type"),
        F.col("student_id").alias("key_id"),
        "enrolled_at",
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") > 5.9) & (F.col("grade_status") == "PASS"))
    .select(
        F.lit("enrollment_2").alias("record_type"),
        F.col("id").alias("key_id"),
        "enrolled_at",
    )
)

result = enrollment_side.union(enrollment_rows)
