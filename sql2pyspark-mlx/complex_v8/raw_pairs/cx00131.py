from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("cohort") != "C2021")
    .select("id")
)

course_side = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("B%")) & (F.col("credits").between(2, 3)))
    .select("id")
)

result = (
    student_side
    .intersect(course_side)
    .orderBy("id")
)
