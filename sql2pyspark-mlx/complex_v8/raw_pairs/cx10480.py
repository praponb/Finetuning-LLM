from pyspark.sql import functions as F

student_side = df_students.select("id", "enrolled_on")

student_side_2 = (
    df_students
    .filter((F.col("enrolled_on") >= F.lit("2024-03-15").cast("date")) & (F.col("gpa") < 3.2))
    .select("id", "enrolled_on")
)

student_rows = df_students.select("id", "enrolled_on")

result = (
    student_side
    .unionByName(student_side_2)
    .unionByName(student_rows)
)
