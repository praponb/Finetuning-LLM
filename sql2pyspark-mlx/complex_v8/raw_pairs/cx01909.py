from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select("term", "id")
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("course_id") <= 9) & (F.col("score") >= 34.2))
    .select("term", F.col("course_id").alias("id"))
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
    .orderBy("term")
)
