from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("grade_status") != "INCOMPLETE"))
    .select(
        F.col("student_id").alias("key_id"),
        F.col("enrolled_at").alias("happened_at"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("grade_status") == "FAIL"))
    .select(
        F.col("student_id").alias("key_id"),
        F.col("enrolled_at").alias("happened_at"),
    )
)

result = (
    enrollment_side
    .unionByName(enrollment_side_2)
    .orderBy("happened_at")
)
