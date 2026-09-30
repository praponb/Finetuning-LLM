from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.col("dept").isNotNull())
    .select(F.col("id").alias("key_id"), F.col("credits").alias("qty"))
)

enrollment_side = df_enrollments.select("id", "course_id")

result = (
    course_rows
    .union(enrollment_side)
    .orderBy("key_id")
)
