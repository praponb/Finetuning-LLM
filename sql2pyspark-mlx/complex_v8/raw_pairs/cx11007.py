from pyspark.sql import functions as F

course_side = (
    df_courses
    .select(F.lit("course").alias("origin"), "credits", F.col("level").alias("label"))
)

student_rows = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-05-18").cast("date"))
    .select(F.lit("student").alias("origin"), "id", "student_name")
)

result = (
    course_side
    .union(student_rows)
    .distinct()
    .orderBy(F.col("credits").desc(), "origin")
)
