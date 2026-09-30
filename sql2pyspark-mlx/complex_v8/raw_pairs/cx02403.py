from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.col("level") == "intro")
    .select("level")
)

course_rows_2 = df_courses.select(F.col("dept").alias("level"))

result = course_rows.intersectAll(course_rows_2)
