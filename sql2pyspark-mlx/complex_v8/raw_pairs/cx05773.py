from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (~F.col("term").isin("2025T1", "2025T2")))
    .select(
        F.col("term").alias("category"),
        "enrolled_at",
        F.col("grade_status").alias("label"),
        F.lit("enrollment").alias("record_type"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "grade_status",
        "enrolled_at",
        "term",
        F.lit("enrollment_2").alias("record_type"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "term",
        "enrolled_at",
        "grade_status",
        F.lit("enrollment_3").alias("record_type"),
    )
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_rows)
    .orderBy(F.col("label").desc())
)
