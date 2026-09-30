from pyspark.sql import functions as F

cleaned = (
    df_students
    .filter((F.col("major").isin("Bio", "Biz", "Math")) & (F.col("gpa").between(2.1, 3.4)))
)

student_rows = cleaned.select("id")

student_side = df_students.select("id")

result = (
    student_rows
    .intersect(student_side)
    .orderBy(F.col("id").desc())
)
