from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 9) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

enrollment_side = (
    valid_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.hour(F.col("enrolled_at")).between(9, 15)))
    .select(F.col("id").alias("key_id"), "enrolled_at")
)

enrollment_side_2 = df_enrollments.select("course_id", "enrolled_at")

enrollment_rows = (
    df_enrollments
    .filter((F.col("term").isin("2025T2", "2025T1", "2024T1")) & (F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE", "PASS")))
    .select("student_id", "enrolled_at")
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_rows)
    .orderBy("key_id", "enrolled_at")
)
