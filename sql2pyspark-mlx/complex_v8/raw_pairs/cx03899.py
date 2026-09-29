from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("course_id").alias("ref_id"),
        F.col("grade_status").alias("label"),
    )
)

enrollment_side_2 = df_enrollments.select("student_id", "term")

result = enrollment_side.subtract(enrollment_side_2)
