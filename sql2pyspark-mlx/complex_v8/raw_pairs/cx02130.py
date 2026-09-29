from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("course_id") <= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 57.0, "large").when(F.col("score") >= 43.5, "medium").otherwise("small").alias("score_tier"),
    )
)

enrollment_side = (
    base
    .filter(F.col("term") == "2024T1")
    .select("term", F.col("enrolled_at").alias("event_time"), "score_tier")
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 14:00:00").cast("timestamp"))
    .select(
        "term",
        F.col("enrolled_at").alias("event_time"),
        F.col("grade_status").alias("score_tier"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .select(
        F.col("grade_status").alias("term"),
        F.col("enrolled_at").alias("event_time"),
        F.col("term").alias("score_tier"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .union(enrollment_side_2)
)
