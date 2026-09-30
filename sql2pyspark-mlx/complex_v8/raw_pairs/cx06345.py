from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter(F.col("credits") <= 3)
    .select(F.col("id").alias("ref_id"))
)

student_side = (
    df_students
    .filter(F.col("gpa") > 2.0)
    .select("id")
)

result = (
    course_side
    .intersectAll(student_side)
    .orderBy(F.col("ref_id").desc())
)
