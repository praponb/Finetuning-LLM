from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("gpa") < 3.4) & (F.lower(F.col("student_name")).like("% s%")))
    .select("id", "enrolled_on")
)

student_rows = (
    df_students
    .filter((F.col("is_international")) & (F.col("gpa").between(2.7, 3.2)))
    .select("id", "enrolled_on")
)

result = (
    student_side
    .union(student_rows)
    .distinct()
)
