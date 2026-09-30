from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("grade_status", "score", "id")

student_rows = df_students.select("major", "gpa", "id")

stacked = (
    enrollment_rows
    .union(student_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("grade_status")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("grade_status")
)
