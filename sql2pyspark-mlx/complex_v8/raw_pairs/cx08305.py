from pyspark.sql import functions as F

course_side = df_courses.select(F.col("level").alias("category"))

course_rows = df_courses.select("level")

result = (
    course_side
    .subtract(course_rows)
    .orderBy("category")
)
