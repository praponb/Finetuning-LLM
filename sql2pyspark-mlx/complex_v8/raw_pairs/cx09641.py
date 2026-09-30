from pyspark.sql import functions as F

student_side = df_students.select("major", F.col("student_name").alias("category"))

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("student_id").isNotNull()))
    .select(F.col("term").alias("major"), F.col("grade_status").alias("category"))
)

result = (
    student_side
    .union(enrollment_side)
    .distinct()
    .orderBy(F.col("category").desc())
)
