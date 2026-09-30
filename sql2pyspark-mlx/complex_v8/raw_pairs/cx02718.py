from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("id", "enrolled_at")

enrollment_rows = df_enrollments.select("student_id", "enrolled_at")

result = (
    enrollment_side
    .union(enrollment_rows)
    .orderBy(F.col("id").desc())
)
