from pyspark.sql import functions as F

valid = (
    df_students
    .filter((F.col("cohort") == "C2024") & (F.upper(F.trim(F.col("student_name"))).like("L%")))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        F.add_months(F.col("enrolled_on"), 12).alias("enrolled_on_plus_12m"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.course_id") >= 3), "left_anti")
)

filtered = (
    qualified.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")), "left_semi")
)

result = (
    filtered
    .filter(F.col("enrolled_on_plus_12m").isNotNull())
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "enrolled_on_plus_12m",
    )
    .orderBy(F.col("enrolled_on_plus_12m").desc())
)
