from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2025T1"))
    .select(F.col("id").alias("entity_id"), "enrolled_at")
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("grade_status").isNotNull()) & (~F.col("term").isin("2025T1", "2024T1")))
    .select("id", "enrolled_at")
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select("id", "enrolled_at")
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .union(enrollment_side_2)
)
