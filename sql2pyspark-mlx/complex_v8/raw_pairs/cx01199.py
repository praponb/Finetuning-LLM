from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
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

enrollment_rows = (
    cleaned_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("score") <= 73.0))
    .select(F.col("student_id").alias("key_id"), "grade_status")
)

enrollment_side = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id") >= 8))
    .select(F.col("student_id").alias("key_id"), "grade_status")
)

result = enrollment_rows.intersect(enrollment_side)
