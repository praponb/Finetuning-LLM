from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("course_id", F.col("score").alias("value"))

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("grade_status") != "PASS")
    .select("id", "score")
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
)
