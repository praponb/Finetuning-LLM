from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select(F.col("term").alias("category"))

course_side = (
    df_courses
    .filter(F.col("level").isin("advanced", "core"))
    .select("level")
)

result = (
    enrollment_rows
    .subtract(course_side)
    .orderBy(F.col("category").desc())
)
