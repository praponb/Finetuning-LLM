from pyspark.sql import functions as F

course_side = df_courses.select(F.col("level").alias("name"))

student_rows = (
    df_students
    .filter((F.col("major") != "Biz") & (F.col("cohort") == "C2022"))
    .select("student_name")
)

result = (
    course_side
    .subtract(student_rows)
    .orderBy("name")
)
