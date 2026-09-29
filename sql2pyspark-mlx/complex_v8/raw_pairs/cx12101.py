from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("student_id").isNotNull()))
    .select(
        F.col("course_id").alias("key_id"),
        F.col("score").alias("value"),
        "enrolled_at",
        F.lit("enrollment").alias("source"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("term").isin("2025T1", "2024T1", "2024T2"))
    .select(
        F.col("course_id").alias("key_id"),
        F.col("score").alias("value"),
        "enrolled_at",
        F.lit("enrollment_2").alias("source"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .distinct()
)
