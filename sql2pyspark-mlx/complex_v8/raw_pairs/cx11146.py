from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") < 58.8) & (F.col("enrolled_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
    .select(
        "term",
        F.col("enrolled_at").alias("happened_at"),
        "score",
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_rows_2 = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "INCOMPLETE")) & (F.col("score") > 20.6))
    .select(
        F.col("grade_status").alias("term"),
        F.col("enrolled_at").alias("happened_at"),
        "score",
        F.lit("enrollment_2").alias("origin"),
    )
)

result = (
    enrollment_rows
    .union(enrollment_rows_2)
    .distinct()
)
