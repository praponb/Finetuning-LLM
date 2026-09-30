from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(F.col("grade_status").alias("category"), F.col("term").alias("name"))
)

course_rows = df_courses.select("course_code", "level")

course_side = (
    df_courses
    .filter((F.col("level").isNotNull()) & (F.col("credits") < 3))
    .select("course_code", "dept")
)

result = (
    enrollment_side
    .union(course_rows)
    .union(course_side)
    .orderBy("name", "category")
)
