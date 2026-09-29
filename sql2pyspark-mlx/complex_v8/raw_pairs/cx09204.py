from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("course_id")

student_rows = df_students.select("id")

result = (
    enrollment_side
    .intersect(student_rows)
    .orderBy(F.col("course_id").desc())
)
