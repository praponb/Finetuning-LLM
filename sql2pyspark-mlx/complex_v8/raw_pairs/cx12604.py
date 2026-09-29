from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-03-08").cast("date")) & (F.col("grade_status") != "PASS"))
    .select(F.col("id").alias("qty"), F.col("enrolled_at").alias("event_time"))
)

enrollment_side = df_enrollments.select("student_id", "enrolled_at")

result = (
    enrollment_rows
    .union(enrollment_side)
    .distinct()
    .orderBy("qty", "event_time")
)
