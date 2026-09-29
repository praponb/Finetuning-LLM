from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("cohort").isin("C2022", "C2024", "C2021")) & (F.col("gpa") > 2.7))
    .select("id")
)

enrollment_side = df_enrollments.select("student_id")

result = (
    student_side
    .subtract(enrollment_side)
    .orderBy("id")
)
