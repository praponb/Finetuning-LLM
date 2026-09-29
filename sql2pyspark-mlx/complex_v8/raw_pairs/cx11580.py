from pyspark.sql import functions as F

enrollment_side = df_enrollments.select(F.col("id").alias("key_id"))

course_rows = (
    df_courses
    .filter(F.col("credits").between(2, 4))
    .select(F.col("credits").alias("key_id"))
)

result = (
    enrollment_side
    .subtract(course_rows)
    .orderBy("key_id")
)
