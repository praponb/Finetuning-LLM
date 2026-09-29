from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("course_id") >= 9))
    .select(F.lit("enrollment").alias("record_type"), "id", "enrolled_at")
)

enrollment_side = (
    df_enrollments
    .select(F.lit("enrollment_2").alias("record_type"), "student_id", "enrolled_at")
)

result = (
    enrollment_rows
    .union(enrollment_side)
    .orderBy("id", "enrolled_at")
)
