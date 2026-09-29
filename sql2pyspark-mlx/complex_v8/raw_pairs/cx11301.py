from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        F.col("id").alias("qty"),
        "enrolled_at",
        F.col("grade_status").alias("name"),
    )
)

enrollment_rows_2 = df_enrollments.select("course_id", "enrolled_at", "grade_status")

result = (
    enrollment_rows
    .union(enrollment_rows_2)
    .orderBy("name")
)
