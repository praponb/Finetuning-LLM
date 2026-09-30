from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_rows = (
    filtered
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id") >= 7))
    .select(F.col("student_id").alias("ref_id"))
)

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-03-14 07:00:00").cast("timestamp")) & (F.col("course_id").isNotNull()))
    .select(F.col("student_id").alias("ref_id"))
)

result = enrollment_rows.intersect(enrollment_side)
