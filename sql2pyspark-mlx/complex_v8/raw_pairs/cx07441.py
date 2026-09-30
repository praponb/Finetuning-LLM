from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 8)
    .select("id", F.col("student_name").alias("label"))
)

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-07 12:00:00").cast("timestamp")) & (F.col("score") > 38.1))
    .select("id", F.col("term").alias("label"))
)

result = student_side.unionByName(enrollment_side)
