from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("score") <= 73.7) & (F.col("course_id").isNotNull()))
    .select(F.col("term").alias("category"))
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("course_id") >= 11)
    .select("grade_status")
)

result = enrollment_side.subtract(enrollment_side_2)
