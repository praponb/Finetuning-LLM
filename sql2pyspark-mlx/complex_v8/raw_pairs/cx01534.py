from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("student_name").isNotNull()) & (~F.col("is_international")))
    .select("id")
)

student_side = df_students.select("id")

result = (
    student_rows
    .exceptAll(student_side)
    .orderBy("id")
)
