from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2024T1", "2025T1"))
    .select(
        F.col("id").alias("ref_id"),
        "enrolled_at",
        F.col("course_id").alias("entity_id"),
    )
)

enrollment_side_2 = df_enrollments.select("student_id", "enrolled_at", "id")

enrollment_rows = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select("student_id", "enrolled_at", "id")
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_rows)
    .orderBy("entity_id")
)
