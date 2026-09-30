from pyspark.sql import functions as F

course_rows = df_courses.select("level", "course_code", F.col("id").alias("qty"))

enrollment_rows = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(6, 11))
    .select("grade_status", "term", "id")
)

result = (
    course_rows
    .union(enrollment_rows)
    .orderBy(F.col("level").desc())
)
