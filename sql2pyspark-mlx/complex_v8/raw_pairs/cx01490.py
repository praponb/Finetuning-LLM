from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

enrollment_rows = (
    valid
    .filter(F.col("student_id") <= 3)
    .select("student_id")
)

course_rows = (
    df_courses
    .filter(F.col("dept") == "Bio")
    .select(F.col("credits").alias("student_id"))
)

result = (
    enrollment_rows
    .intersectAll(course_rows)
    .orderBy("student_id")
)
