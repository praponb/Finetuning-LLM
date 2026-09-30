from pyspark.sql import functions as F

cleaned_enrollments = (
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

result = (
    cleaned_enrollments
    .filter(~F.col("term").isin("2024T1", "2025T1"))
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(10)
)
