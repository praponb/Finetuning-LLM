from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("is_international"))
    .select("student_name")
)

course_side = (
    df_courses
    .filter((F.col("course_code").isNotNull()) & (F.col("dept").isin("Biz", "CS")))
    .select(F.col("course_code").alias("student_name"))
)

result = student_side.intersect(course_side)
