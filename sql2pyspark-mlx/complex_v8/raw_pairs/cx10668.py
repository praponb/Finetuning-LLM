from pyspark.sql import functions as F

student_side = df_students.select(F.col("cohort").alias("label"))

course_side = df_courses.select(F.col("level").alias("label"))

result = (
    student_side
    .subtract(course_side)
    .orderBy("label")
)
