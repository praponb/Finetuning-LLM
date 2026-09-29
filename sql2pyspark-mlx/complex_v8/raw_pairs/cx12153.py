from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 06:00:00").cast("timestamp"))
    .select("student_id")
)

student_rows = (
    df_students
    .filter((F.col("gpa") >= 2.0) & (F.col("major") == "Art"))
    .select("id")
)

result = enrollment_side.subtract(student_rows)
