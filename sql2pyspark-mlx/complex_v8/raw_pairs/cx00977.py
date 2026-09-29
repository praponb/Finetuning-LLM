from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        F.col("student_id").alias("qty"),
        F.col("id").alias("key_id"),
        F.col("enrolled_at").alias("happened_at"),
        F.lit("enrollment").alias("src"),
    )
)

enrollment_side = (
    df_enrollments
    .select(
        "student_id",
        "course_id",
        "enrolled_at",
        F.lit("enrollment_2").alias("src"),
    )
)

result = enrollment_rows.union(enrollment_side)
