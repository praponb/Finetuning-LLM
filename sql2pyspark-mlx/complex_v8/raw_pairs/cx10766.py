from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("term") == "2025T2") & (F.col("student_id").isNotNull()))
    .select(
        "course_id",
        "enrolled_at",
        "id",
        F.lit("enrollment").alias("source"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("grade_status").isin("WITHDRAWN", "PASS", "FAIL")) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        F.col("student_id").alias("course_id"),
        "enrolled_at",
        "id",
        F.lit("enrollment_2").alias("source"),
    )
)

result = (
    enrollment_side
    .unionByName(enrollment_rows)
    .distinct()
    .orderBy("source", "id")
)
