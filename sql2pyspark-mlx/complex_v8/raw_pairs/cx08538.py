from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(~F.col("term").isin("2025T1", "2024T2"))
    .select("course_id")
)

student_side = (
    df_students
    .filter((F.col("is_international")) & (F.col("gpa") >= 2.7))
    .select(F.col("id").alias("course_id"))
)

result = enrollment_side.intersectAll(student_side)
