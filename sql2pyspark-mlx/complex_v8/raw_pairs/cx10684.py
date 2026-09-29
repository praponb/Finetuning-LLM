from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("course_id") >= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_side = (
    df_enrollments
    .select(
        "term",
        "enrolled_at",
        "grade_status",
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-12 09:00:00").cast("timestamp"))
    .select(
        "term",
        "enrolled_at",
        "grade_status",
        F.lit("enrollment_2").alias("origin"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .distinct()
)
