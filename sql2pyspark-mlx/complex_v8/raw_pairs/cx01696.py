from pyspark.sql import functions as F

scoped_enrollments = df_enrollments

enrollment_side = scoped_enrollments.select("grade_status", F.col("enrolled_at").alias("happened_at"))

enrollment_side_2 = (
    df_enrollments
    .select(
        F.col("term").alias("grade_status"),
        F.col("enrolled_at").alias("happened_at"),
    )
)

result = (
    enrollment_side
    .unionByName(enrollment_side_2)
    .distinct()
    .orderBy("happened_at", "grade_status")
)
