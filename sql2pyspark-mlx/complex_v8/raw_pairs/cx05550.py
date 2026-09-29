from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 10.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
    )
)

enrollment_rows = (
    cleaned_enrollments
    .filter(F.col("term") == "2025T2")
    .select(F.col("student_id").alias("entity_id"))
)

enrollment_rows_2 = df_enrollments.select("course_id")

result = (
    enrollment_rows
    .subtract(enrollment_rows_2)
    .orderBy(F.col("entity_id").desc())
)
