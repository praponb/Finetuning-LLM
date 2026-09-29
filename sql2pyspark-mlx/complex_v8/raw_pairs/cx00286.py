from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("id", F.col("score").alias("value"), "enrolled_at")

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("course_id") >= 6) & (F.hour(F.col("enrolled_at")).between(8, 14)))
    .select(
        F.col("course_id").alias("id"),
        F.col("score").alias("value"),
        "enrolled_at",
    )
)

result = enrollment_side.union(enrollment_side_2)
