from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter(F.col("level") == "core")
    .select(F.col("credits").alias("qty"))
)

enrollment_side = df_enrollments.select(F.col("course_id").alias("qty"))

result = (
    course_side
    .intersectAll(enrollment_side)
    .orderBy("qty")
)
