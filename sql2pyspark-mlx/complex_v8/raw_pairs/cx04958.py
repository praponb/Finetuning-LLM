from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("grade_status") == "FAIL"))
    .select(F.col("grade_status").alias("category"), "term")
)

student_side = (
    df_students
    .filter((F.col("cohort") == "C2022") & (F.col("is_international")))
    .select("student_name", "major")
)

student_rows = (
    df_students
    .filter((~F.col("is_international")) & (F.col("gpa") < 3.4))
    .select("major", "student_name")
)

result = (
    enrollment_side
    .union(student_side)
    .union(student_rows)
    .orderBy("category")
)
