from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("gpa") <= 3.4)
    .select(F.col("id").alias("key_id"))
)

course_side = df_courses.select("credits")

result = student_rows.intersectAll(course_side)
