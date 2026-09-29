from pyspark.sql import functions as F

student_side = df_students.select(F.col("major").alias("label"))

course_rows = df_courses.select(F.col("dept").alias("label"))

result = (
    student_side
    .intersectAll(course_rows)
    .orderBy("label")
)
