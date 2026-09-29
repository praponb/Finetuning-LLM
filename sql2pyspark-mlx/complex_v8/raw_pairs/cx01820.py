from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.col("term").isin("2024T2", "2024T1")) & (F.col("student_id") >= 17))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enrollment_side = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status") != "FAIL"))
    .select(
        F.lit("enrollment").alias("origin"),
        F.col("course_id").alias("qty"),
        F.col("enrolled_at").alias("event_time"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(F.lit("enrollment_2").alias("origin"), "student_id", "enrolled_at")
)

unioned = enrollment_side.union(enrollment_side_2)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("origin")
)
