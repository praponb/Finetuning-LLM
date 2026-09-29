from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.col("dept") == "Biz") & (F.col("credits") <= 3))
    .select(F.col("credits").alias("key_id"), "course_code")
)

course_rows = df_courses.select(F.col("id").alias("key_id"), F.col("dept").alias("course_code"))

result = (
    course_side
    .subtract(course_rows)
    .orderBy("course_code")
)
