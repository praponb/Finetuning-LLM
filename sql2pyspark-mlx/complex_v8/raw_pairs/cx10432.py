from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("level") == "core"))
    .select(F.col("course_code").alias("name"))
)

course_side = df_courses.select("course_code")

result = (
    course_rows
    .intersectAll(course_side)
    .orderBy("name")
)
