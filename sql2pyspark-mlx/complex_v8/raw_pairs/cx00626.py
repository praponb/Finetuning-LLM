from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("term") == "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

qualified = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.term").isin("2024T2", "2025T2")) & (F.col("e.course_id").isNotNull()), "left_anti")
)

annotated = (
    qualified
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .withColumn("grade_status_clean", F.lower(F.trim(F.col("grade_status"))))
)

result = (
    annotated
    .filter(F.col("student_id") >= 13)
    .select(
        "id",
        "student_id",
        "grade_status",
        "grade_status_clean",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
    )
    .orderBy(F.col("id").desc())
)
