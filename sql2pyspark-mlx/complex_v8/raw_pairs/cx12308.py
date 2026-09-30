from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("id")

course_rows = (
    df_courses
    .filter(F.col("dept").isNotNull())
    .select(F.col("credits").alias("id"))
)

result = enrollment_rows.intersectAll(course_rows)
