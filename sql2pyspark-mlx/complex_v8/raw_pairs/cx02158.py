from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("grade_status").alias("label"),
        F.col("enrolled_at").alias("event_time"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(F.col("term").alias("label"), F.col("enrolled_at").alias("event_time"))
)

all_events = enrollment_side.union(enrollment_side_2)

result = (
    all_events
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("label")
)
