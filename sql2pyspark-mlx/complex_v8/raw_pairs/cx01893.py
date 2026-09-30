from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("gpa") <= 3.2)
    .select(F.col("id").alias("ref_id"))
)

student_rows_2 = (
    df_students
    .filter(~F.col("is_international"))
    .select("id")
)

result = (
    student_rows
    .intersect(student_rows_2)
    .orderBy("ref_id")
)
