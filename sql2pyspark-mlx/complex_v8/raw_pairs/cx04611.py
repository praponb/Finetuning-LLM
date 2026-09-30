from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("score") <= 63.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_side = (
    scoped
    .filter((F.col("score") >= 32.7) & (F.col("course_id") <= 8))
    .select(
        F.col("course_id").alias("qty"),
        F.col("term").alias("name"),
        "enrolled_at",
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2025T1", "2024T1"))
    .select("student_id", "term", "enrolled_at")
)

enrollment_side_3 = df_enrollments.select("student_id", "term", "enrolled_at")

result = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_side_3)
)
