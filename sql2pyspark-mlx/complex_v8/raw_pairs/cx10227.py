from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("gpa") < 3.4)
    .select("id", "student_name")
)

student_rows_2 = df_students.select("id", F.col("major").alias("student_name"))

result = (
    student_rows
    .exceptAll(student_rows_2)
    .orderBy(F.col("student_name").desc())
)
