from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

enrollment_rows = (
    filtered
    .select(
        F.lit("enrollment").alias("source"),
        F.col("grade_status").alias("category"),
        F.col("enrolled_at").alias("event_time"),
    )
)

enrollment_side = df_enrollments.select(F.lit("enrollment_2").alias("source"), "term", "enrolled_at")

all_events = enrollment_rows.union(enrollment_side)

result = (
    all_events
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)
