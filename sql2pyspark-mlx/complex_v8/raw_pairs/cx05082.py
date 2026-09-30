from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        F.col("grade_status").alias("category"),
        F.col("score").alias("value"),
        "term",
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")) & (F.col("score") < 78.8))
    .select("grade_status", "score", "term")
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .orderBy("value")
)
