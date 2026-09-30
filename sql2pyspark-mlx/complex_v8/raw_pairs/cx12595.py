from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 2)) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "term",
        "score",
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_rows = (
    df_enrollments
    .select(
        "id",
        "grade_status",
        "score",
        F.lit("enrollment_2").alias("origin"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .orderBy(F.col("origin").desc())
)
