from pyspark.sql import functions as F

student_rows = df_students.select(F.col("id").alias("ref_id"))

enrollment_side = df_enrollments.select(F.col("id").alias("ref_id"))

result = (
    student_rows
    .intersect(enrollment_side)
    .orderBy("ref_id")
)
