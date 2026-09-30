from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("score").between(26.2, 85.8))
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

enrollment_rows = (
    cleaned_enrollments
    .select(
        F.col("grade_status").alias("name"),
        "score",
        F.lit("enrollment").alias("origin"),
    )
)

student_rows = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("H%"))
    .select(
        F.col("cohort").alias("name"),
        F.col("gpa").alias("score"),
        F.lit("student").alias("origin"),
    )
)

enrollment_side = (
    df_enrollments
    .select(
        F.col("term").alias("name"),
        "score",
        F.lit("enrollment_3").alias("origin"),
    )
)

result = (
    enrollment_rows
    .union(student_rows)
    .union(enrollment_side)
    .orderBy("origin", "score")
)
