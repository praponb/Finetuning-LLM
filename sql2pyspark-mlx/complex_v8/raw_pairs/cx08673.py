from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(~F.col("grade_status").isin("INCOMPLETE", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

enrollment_side = (
    cleaned_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("course_id").isNotNull()))
    .select("id", "enrolled_at")
)

enrollment_side_2 = df_enrollments.select("id", "enrolled_at")

enrollment_side_3 = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 06:00:00").cast("timestamp"))
    .select("student_id", "enrolled_at")
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_side_3)
)
