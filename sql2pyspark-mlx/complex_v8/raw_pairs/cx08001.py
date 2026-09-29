from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

result = (
    cleaned_enrollments
    .filter(~F.col("term").isin("2025T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
    )
    .orderBy(F.col("id").asc())
    .limit(20)
)
