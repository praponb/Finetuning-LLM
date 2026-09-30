from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("student_id")

course_rows = df_courses.select("id")

result = (
    enrollment_rows
    .exceptAll(course_rows)
    .orderBy(F.col("student_id").desc())
)
