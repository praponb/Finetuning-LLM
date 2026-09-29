from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-02-18").cast("date"))
    .select(F.col("grade_status").alias("category"))
)

student_rows = df_students.select("major")

result = (
    enrollment_rows
    .intersect(student_rows)
    .orderBy("category")
)
