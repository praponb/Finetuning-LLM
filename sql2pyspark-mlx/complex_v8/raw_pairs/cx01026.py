from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

enrollment_side = df_enrollments.select("student_id", "enrolled_at")

enrollment_rows = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 3)) & (F.col("term") == "2025T2"))
    .select(F.col("course_id").alias("student_id"), "enrolled_at")
)

result = (
    enrollment_side
    .unionByName(enrollment_rows)
    .orderBy("enrolled_at", "student_id")
)
