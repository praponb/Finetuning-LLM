from pyspark.sql import functions as F

student_side = df_students.select("id")

course_side = (
    df_courses
    .filter(F.col("dept") == "Bio")
    .select("id")
)

result = (
    student_side
    .exceptAll(course_side)
    .orderBy(F.col("id").desc())
)
