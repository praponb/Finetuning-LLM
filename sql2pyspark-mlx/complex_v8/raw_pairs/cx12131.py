from pyspark.sql import functions as F

course_side = df_courses.select("id", F.col("dept").alias("name"))

student_side = df_students.select("id", F.col("major").alias("name"))

result = (
    course_side
    .exceptAll(student_side)
    .orderBy(F.col("id").desc())
)
