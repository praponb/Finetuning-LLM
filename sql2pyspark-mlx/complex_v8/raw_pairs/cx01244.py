from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("grade_status").alias("label"),
        F.col("enrolled_at").alias("event_time"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("course_id") >= 4) & (F.col("enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")))
    .select(F.col("term").alias("label"), F.col("enrolled_at").alias("event_time"))
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.hour(F.col("enrolled_at")).between(6, 12)) & (F.col("term") == "2024T1"))
    .select(F.col("term").alias("label"), F.col("enrolled_at").alias("event_time"))
)

result = (
    enrollment_side
    .unionByName(enrollment_rows)
    .unionByName(enrollment_side_2)
)
