from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.col("course_code").isNotNull()) & (F.col("dept") != "Art"))
    .select(F.col("id").alias("ref_id"))
)

course_side_2 = df_courses.select("credits")

result = course_side.exceptAll(course_side_2)
