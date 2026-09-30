from pyspark.sql import functions as F

enrollment_side = df_enrollments.select(F.col("id").alias("qty"), "student_id")

course_rows = df_courses.select("id", "credits")

result = (
    enrollment_side
    .union(course_rows)
    .orderBy(F.col("student_id").desc())
)
