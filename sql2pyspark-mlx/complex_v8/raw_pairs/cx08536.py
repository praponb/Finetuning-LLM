from pyspark.sql import functions as F

student_rows = df_students.select(F.col("major").alias("category"), "gpa")

student_side = df_students.select(F.col("student_name").alias("category"), "gpa")

result = (
    student_rows
    .unionByName(student_side)
    .orderBy("gpa")
)
