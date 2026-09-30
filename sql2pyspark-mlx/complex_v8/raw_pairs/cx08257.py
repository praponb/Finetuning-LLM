from pyspark.sql import functions as F

student_side = df_students.select(F.col("id").alias("ref_id"), "student_name")

course_side = (
    df_courses
    .filter(F.col("dept") == "Math")
    .select(F.col("credits").alias("ref_id"), F.col("dept").alias("student_name"))
)

result = (
    student_side
    .union(course_side)
    .orderBy(F.col("ref_id").desc(), "student_name")
)
