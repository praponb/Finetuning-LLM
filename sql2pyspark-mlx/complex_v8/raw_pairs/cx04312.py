from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select("course_id", F.col("enrolled_at").alias("event_time"), "score")
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select("course_id", "enrolled_at", "score")
)

all_events = enrollment_side.union(enrollment_rows)

result = (
    all_events
    .groupBy("course_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("score").alias("total_score"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("course_id")
)
