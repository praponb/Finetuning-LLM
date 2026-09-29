from pyspark.sql import functions as F

student_rows = df_students.select(F.col("id").alias("qty"), "enrolled_on")

student_rows_2 = (
    df_students
    .filter(F.col("gpa").between(1.7, 3.2))
    .select("id", "enrolled_on")
)

result = student_rows.union(student_rows_2)
