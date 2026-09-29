from pyspark.sql import functions as F

student_side = df_students.select("id", F.col("major").alias("category"))

course_side = df_courses.select("id", F.col("level").alias("category"))

result = (
    student_side
    .subtract(course_side)
    .orderBy(F.col("category").desc())
)
