from pyspark.sql import functions as F

course_rows = df_courses.select(F.col("credits").alias("ref_id"))

course_rows_2 = (
    df_courses
    .filter(F.col("dept").isin("Art", "Bio", "Biz"))
    .select("id")
)

result = (
    course_rows
    .subtract(course_rows_2)
    .orderBy("ref_id")
)
