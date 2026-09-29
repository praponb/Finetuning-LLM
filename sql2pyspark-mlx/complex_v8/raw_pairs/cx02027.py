from pyspark.sql import functions as F

course_rows = df_courses.select("id")

student_side = df_students.select("id")

result = (
    course_rows
    .intersect(student_side)
    .orderBy(F.col("id").desc())
)
