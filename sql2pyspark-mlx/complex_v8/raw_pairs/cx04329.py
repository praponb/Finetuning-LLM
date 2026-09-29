from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.col("term") == "2024T2")
    .select("course_id")
)

course_rows = (
    df_courses
    .filter(F.col("level") == "core")
    .select(F.col("id").alias("course_id"))
)

result = (
    enrollment_rows
    .intersect(course_rows)
    .orderBy("course_id")
)
