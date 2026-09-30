from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") > 13.9) & (F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")))
    .select("grade_status", "term", "enrolled_at")
)

enrollment_rows_2 = (
    df_enrollments
    .select(
        F.col("term").alias("grade_status"),
        F.col("grade_status").alias("term"),
        "enrolled_at",
    )
)

result = enrollment_rows.unionByName(enrollment_rows_2)
