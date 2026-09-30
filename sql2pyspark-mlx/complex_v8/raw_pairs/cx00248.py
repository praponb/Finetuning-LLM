from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("id", "score", F.col("enrolled_at").alias("happened_at"))

enrollment_side = df_enrollments.select("student_id", "score", "enrolled_at")

result = (
    enrollment_rows
    .union(enrollment_side)
    .distinct()
    .orderBy("happened_at")
)
