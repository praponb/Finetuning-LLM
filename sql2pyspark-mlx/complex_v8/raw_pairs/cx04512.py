from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((~F.col("term").isin("2024T1", "2025T1")) & (F.col("student_id").isNotNull()))
    .select(
        F.col("term").alias("name"),
        F.col("id").alias("key_id"),
        F.col("enrolled_at").alias("happened_at"),
    )
)

enrollment_side = (
    df_enrollments
    .select(
        F.col("grade_status").alias("name"),
        F.col("id").alias("key_id"),
        F.col("enrolled_at").alias("happened_at"),
    )
)

result = (
    enrollment_rows
    .union(enrollment_side)
    .orderBy("happened_at")
)
