from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("id")

course_side = (
    df_courses
    .filter(F.col("dept") == "Biz")
    .select("credits")
)

result = (
    enrollment_rows
    .intersect(course_side)
    .orderBy("id")
)
