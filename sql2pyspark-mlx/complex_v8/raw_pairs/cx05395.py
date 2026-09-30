from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 4) & (~F.col("dept").isin("Art", "Biz")))
    .select(F.col("id").alias("key_id"), "dept")
)

course_side_2 = df_courses.select(F.col("id").alias("key_id"), F.col("level").alias("dept"))

result = (
    course_side
    .subtract(course_side_2)
    .orderBy("dept", "key_id")
)
