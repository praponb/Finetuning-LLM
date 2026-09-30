from pyspark.sql import functions as F

student_side = df_students.select("id", "major")

enrollment_rows = df_enrollments.select(F.col("course_id").alias("id"), F.col("term").alias("major"))

course_rows = df_courses.select(F.col("credits").alias("id"), F.col("course_code").alias("major"))

result = (
    student_side
    .union(enrollment_rows)
    .union(course_rows)
)
