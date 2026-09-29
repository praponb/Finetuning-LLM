from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("grade_status")

enrollment_side = df_enrollments.select(F.col("term").alias("grade_status"))

result = (
    enrollment_rows
    .intersect(enrollment_side)
    .orderBy("grade_status")
)
