from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("score").between(32.7, 76.8))
    .select(
        F.col("course_id").alias("ref_id"),
        F.col("term").alias("name"),
        F.col("enrolled_at").alias("event_time"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(
        F.col("course_id").alias("ref_id"),
        F.col("grade_status").alias("name"),
        F.col("enrolled_at").alias("event_time"),
    )
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
)
