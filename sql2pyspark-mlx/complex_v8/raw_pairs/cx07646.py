from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp"))
    .select(
        F.col("term").alias("name"),
        F.col("enrolled_at").alias("event_time"),
        "score",
    )
)

enrollment_side_2 = df_enrollments.select("term", "enrolled_at", "score")

enrollment_rows = df_enrollments.select("grade_status", "enrolled_at", "score")

unioned = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_rows)
)

result = (
    unioned
    .groupBy("name")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("score").alias("total_score"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("name")
)
