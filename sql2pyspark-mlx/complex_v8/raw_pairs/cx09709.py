from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("PASS", "WITHDRAWN")) | (F.month(F.col("enrolled_at")).isin(1, 3)))
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

enrollment_rows = cleaned_enrollments.select(F.col("student_id").alias("key_id"), "score")

student_side = df_students.select("id", "gpa")

result = (
    enrollment_rows
    .union(student_side)
    .distinct()
)
