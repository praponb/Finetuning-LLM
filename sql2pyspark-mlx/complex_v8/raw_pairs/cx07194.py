from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("enrolled_at") >= F.lit("2025-01-07 11:00:00").cast("timestamp")))
    .select(F.col("student_id").alias("qty"), "enrolled_at")
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select("student_id", "enrolled_at")
)

all_events = enrollment_side.union(enrollment_side_2)

result = (
    all_events
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_at").alias("latest_enrolled_at"),
    )
    .orderBy("qty")
)
