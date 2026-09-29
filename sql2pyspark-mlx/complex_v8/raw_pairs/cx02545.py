from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("major") == "Math")
    .select(F.col("major").alias("name"))
)

course_side = (
    df_courses
    .filter(F.col("dept") == "Math")
    .select(F.col("course_code").alias("name"))
)

result = (
    student_rows
    .subtract(course_side)
    .orderBy(F.col("name").desc())
)
