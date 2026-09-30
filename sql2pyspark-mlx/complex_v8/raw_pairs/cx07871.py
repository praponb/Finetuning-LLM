from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("student_id") >= 19))
    .select("course_id", F.col("enrolled_at").alias("event_time"))
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select("student_id", "enrolled_at")
)

stacked = enrollment_side.union(enrollment_rows)

result = (
    stacked
    .groupBy("course_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("course_id")
)
