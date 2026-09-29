from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-12 12:00:00").cast("timestamp"))
    .select("course_id", "enrolled_at", F.col("score").alias("metric"))
)

enrollment_side_2 = df_enrollments.select("course_id", "enrolled_at", "score")

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
    .orderBy("metric")
)
