from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("id", F.col("term").alias("name"))

course_side = (
    df_courses
    .filter(F.length(F.trim(F.col("course_code"))) >= 7)
    .select("id", "dept")
)

result = (
    enrollment_rows
    .intersectAll(course_side)
    .orderBy("id")
)
