from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term").isNotNull()) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
    .filter(F.col("course_id") <= 4)
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "term",
        "score",
    )
    .orderBy(F.col("id").desc(), "student_id")
)
