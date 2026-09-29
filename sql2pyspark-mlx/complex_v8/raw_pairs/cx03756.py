from pyspark.sql import functions as F

student_rows = df_students.select(F.col("id").alias("qty"), "enrolled_on")

student_side = (
    df_students
    .filter(F.col("gpa") >= 2.0)
    .select(F.col("id").alias("qty"), "enrolled_on")
)

student_rows_2 = df_students.select(F.col("id").alias("qty"), "enrolled_on")

result = (
    student_rows
    .union(student_side)
    .union(student_rows_2)
    .orderBy(F.col("enrolled_on").desc(), "qty")
)
