from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        "student_id",
        "enrolled_at",
        "score",
        F.lit("enrollment").alias("src"),
    )
)

enrollment_side = (
    df_enrollments
    .select(
        "student_id",
        "enrolled_at",
        "score",
        F.lit("enrollment_2").alias("src"),
    )
)

result = (
    enrollment_rows
    .unionByName(enrollment_side)
    .orderBy(F.col("src").desc())
)
