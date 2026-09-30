from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

enrollment_side = (
    cleaned_enrollments
    .filter((F.col("student_id") >= 14) & (F.hour(F.col("enrolled_at")).between(8, 12)))
    .select(
        F.col("term").alias("label"),
        F.col("enrolled_at").alias("event_time"),
        F.col("score").alias("amount"),
        F.lit("enrollment").alias("source"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(
        F.col("grade_status").alias("label"),
        F.col("enrolled_at").alias("event_time"),
        F.col("score").alias("amount"),
        F.lit("enrollment_2").alias("source"),
    )
)

all_events = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
)

result = (
    all_events
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("amount").alias("total_amount"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)
