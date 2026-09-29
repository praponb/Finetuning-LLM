from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.lit("enrollment").alias("origin"),
        "id",
        "score",
        "grade_status",
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(
        F.lit("enrollment_2").alias("origin"),
        "student_id",
        "score",
        "term",
    )
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .orderBy("id")
)
