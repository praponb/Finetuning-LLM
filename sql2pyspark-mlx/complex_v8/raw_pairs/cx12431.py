from pyspark.sql import functions as F

cleaned = df_enrollments.filter((F.col("student_id").isNotNull()) & (F.col("score") < 79.2))

enrollment_side = (
    cleaned
    .filter(F.col("term") == "2025T2")
    .select("course_id", F.col("grade_status").alias("name"))
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select("student_id", "term")
)

result = (
    enrollment_side
    .exceptAll(enrollment_side_2)
    .orderBy("name")
)
