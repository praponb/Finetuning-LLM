from pyspark.sql import functions as F

course_rows = df_courses.select(F.col("credits").alias("qty"))

student_side = df_students.select("id")

result = (
    course_rows
    .exceptAll(student_side)
    .orderBy(F.col("qty").desc())
)
