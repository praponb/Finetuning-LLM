from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("gpa") >= 2.5)
    .select("id")
)

enrollment_side = df_enrollments.select(F.col("student_id").alias("id"))

result = student_side.intersect(enrollment_side)
