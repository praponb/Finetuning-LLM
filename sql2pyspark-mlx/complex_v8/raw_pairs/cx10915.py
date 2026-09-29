from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.col("term").isin("2024T2", "2024T1"))
    .select(
        "grade_status",
        F.col("enrolled_at").alias("happened_at"),
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("course_id") >= 8))
    .select("grade_status", "enrolled_at", F.lit("enrollment_2").alias("origin"))
)

result = (
    enrollment_rows
    .union(enrollment_side)
    .distinct()
    .orderBy(F.col("happened_at").desc())
)
