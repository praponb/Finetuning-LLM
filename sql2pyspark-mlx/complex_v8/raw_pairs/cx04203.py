from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("C%"))
    .select(
        "course_code",
        F.col("level").alias("label"),
        "id",
        F.lit("course").alias("origin"),
    )
)

enrollment_side = (
    df_enrollments
    .filter((F.col("term") == "2024T2") & (F.col("student_id") >= 4))
    .select(
        "grade_status",
        "term",
        "id",
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "term",
        "grade_status",
        "id",
        F.lit("enrollment").alias("origin"),
    )
)

result = (
    course_rows
    .union(enrollment_side)
    .union(enrollment_side_2)
)
