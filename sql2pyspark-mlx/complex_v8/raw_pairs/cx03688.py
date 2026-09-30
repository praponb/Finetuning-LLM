from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("term").alias("name"),
        "enrolled_at",
        F.col("score").alias("amount"),
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "grade_status",
        "enrolled_at",
        "score",
        F.lit("enrollment_2").alias("origin"),
    )
)

result = enrollment_side.union(enrollment_rows)
