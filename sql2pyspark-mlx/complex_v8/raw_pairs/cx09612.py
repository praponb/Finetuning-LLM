from pyspark.sql import functions as F

recent_students = (
    df_students
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
    )
)

student_rows = (
    recent_students
    .filter(F.col("student_name").isNotNull())
    .select("id")
)

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("grade_status") == "WITHDRAWN"))
    .select(F.col("student_id").alias("id"))
)

result = (
    student_rows
    .exceptAll(enrollment_side)
    .orderBy("id")
)
