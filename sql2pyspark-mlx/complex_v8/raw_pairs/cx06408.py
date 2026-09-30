from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.abs(F.col("score") - 59.7).alias("score_distance"),
    )
)

exploded_detail = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_bucket",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_details = (
    exploded_detail.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.term").isin("2024T1", "2025T2", "2024T2")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_bucket",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_score",
        F.col("e.score").alias("enrollment_score_2"),
    )
)

with_parent_enrollment = (
    with_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .filter(F.col("e.term") == "2025T2")
    .select(
        "s.*",
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    with_parent_enrollment
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_score",
        "enrollment_term",
        "enrollment_grade_status",
    )
    .orderBy("enrollment_grade_status")
)
