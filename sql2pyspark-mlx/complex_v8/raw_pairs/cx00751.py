from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("is_international")) & (F.col("enrolled_on").between(F.lit("2024-02-28").cast("date"), F.lit("2025-07-18").cast("date"))))
    .select(F.col("id").alias("qty"), "student_name", F.col("gpa").alias("value"))
)

enrollment_rows = df_enrollments.select("id", "grade_status", "score")

enrollment_rows_2 = df_enrollments.select("course_id", "grade_status", "score")

result = (
    student_side
    .union(enrollment_rows)
    .union(enrollment_rows_2)
)
