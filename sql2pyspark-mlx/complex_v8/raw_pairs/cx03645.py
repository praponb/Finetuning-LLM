from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter(F.col("course_code").isNotNull())
    .select("id")
)

course_rows = df_courses.select(F.col("credits").alias("id"))

result = (
    course_side
    .intersect(course_rows)
    .orderBy("id")
)
