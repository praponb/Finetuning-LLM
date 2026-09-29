from pyspark.sql import functions as F

student_side = df_students.select("id")

student_rows = (
    df_students
    .filter(F.col("student_name").isNotNull())
    .select("id")
)

result = (
    student_side
    .exceptAll(student_rows)
    .orderBy(F.col("id").desc())
)
