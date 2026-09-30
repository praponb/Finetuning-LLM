from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("term") != "2025T2") & (F.col("enrolled_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(F.col("course_id").alias("key_id"), "enrolled_at")
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("student_id").isNotNull()))
    .select(F.col("id").alias("key_id"), "enrolled_at")
)

result = (
    enrollment_side
    .unionByName(enrollment_side_2)
    .distinct()
)
