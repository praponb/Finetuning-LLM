from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id") <= 17)
    .select(F.col("id").alias("key_id"))
)

student_rows = (
    df_students
    .filter((F.col("enrolled_on").between(F.lit("2024-04-01").cast("date"), F.lit("2025-06-27").cast("date"))) & (F.col("cohort") != "C2021"))
    .select("id")
)

result = (
    enrollment_side
    .intersectAll(student_rows)
    .orderBy(F.col("key_id").desc())
)
