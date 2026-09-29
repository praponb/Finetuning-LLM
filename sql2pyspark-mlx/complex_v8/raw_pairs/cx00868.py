from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("score").between(23.7, 83.1)) & (F.col("term").isin("2025T1", "2024T1", "2025T2")))
    .select(F.col("course_id").alias("entity_id"))
)

student_side = df_students.select("id")

result = enrollment_side.subtract(student_side)
