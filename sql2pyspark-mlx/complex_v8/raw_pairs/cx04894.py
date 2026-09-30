from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        F.col("student_id").alias("ref_id"),
        F.col("grade_status").alias("name"),
        "enrolled_at",
    )
)

enrollment_rows_2 = (
    df_enrollments
    .filter(F.col("score") <= 73.7)
    .select(
        F.col("course_id").alias("ref_id"),
        F.col("term").alias("name"),
        "enrolled_at",
    )
)

result = (
    enrollment_rows
    .unionByName(enrollment_rows_2)
    .distinct()
    .orderBy(F.col("name").desc())
)
