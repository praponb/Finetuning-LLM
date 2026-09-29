from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-04-04").cast("date"))
    .select(
        F.lit("student").alias("origin"),
        F.col("id").alias("ref_id"),
        F.col("enrolled_on").alias("day"),
    )
)

student_side = (
    df_students
    .filter(F.col("is_international"))
    .select(F.lit("student_2").alias("origin"), "id", "enrolled_on")
)

student_rows_2 = df_students.select(F.lit("student_3").alias("origin"), "id", "enrolled_on")

result = (
    student_rows
    .union(student_side)
    .union(student_rows_2)
    .orderBy("day", "origin")
)
