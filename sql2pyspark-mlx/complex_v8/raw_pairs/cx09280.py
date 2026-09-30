from pyspark.sql import functions as F

student_side = df_students.select(F.col("id").alias("key_id"), "student_name")

course_side = (
    df_courses
    .filter(F.col("dept") == "Art")
    .select("credits", "course_code")
)

result = (
    student_side
    .subtract(course_side)
    .orderBy(F.col("student_name").desc(), "key_id")
)
