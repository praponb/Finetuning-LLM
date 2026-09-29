from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("major") == "Art") & (F.upper(F.trim(F.col("student_name"))).like("A%")))
    .select(F.col("id").alias("qty"))
)

student_rows_2 = df_students.select("id")

result = (
    student_rows
    .subtract(student_rows_2)
    .orderBy(F.col("qty").desc())
)
