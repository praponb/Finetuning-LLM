from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("major").isin("Art", "Math")) & (~F.col("is_international")))
    .select(F.col("student_name").alias("category"), "gpa")
)

student_side_2 = (
    df_students
    .filter(F.col("is_international"))
    .select(F.col("cohort").alias("category"), "gpa")
)

enrollment_side = (
    df_enrollments
    .filter(F.col("course_id") >= 6)
    .select(F.col("term").alias("category"), F.col("score").alias("gpa"))
)

result = (
    student_side
    .unionByName(student_side_2)
    .unionByName(enrollment_side)
    .orderBy(F.col("category").desc())
)
