from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 6)
    .select("student_name", F.col("major").alias("label"))
)

enrollment_side = (
    df_enrollments
    .filter(F.col("course_id") >= 9)
    .select(
        F.col("term").alias("student_name"),
        F.col("grade_status").alias("label"),
    )
)

result = student_rows.unionByName(enrollment_side)
