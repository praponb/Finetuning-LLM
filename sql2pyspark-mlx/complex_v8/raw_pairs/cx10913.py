from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id") >= 4)
    .select("term")
)

student_rows = (
    df_students
    .filter((F.col("gpa").between(2.5, 3.2)) & (F.lower(F.col("student_name")).like("% n%")))
    .select(F.col("student_name").alias("term"))
)

result = (
    enrollment_side
    .intersect(student_rows)
    .orderBy(F.col("term").desc())
)
