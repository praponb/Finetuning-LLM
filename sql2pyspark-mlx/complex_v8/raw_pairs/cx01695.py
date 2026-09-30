from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("id", "score")

student_side = df_students.select("id", "gpa")

result = enrollment_rows.union(student_side)
