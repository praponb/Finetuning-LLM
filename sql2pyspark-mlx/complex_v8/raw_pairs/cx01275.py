from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("cohort").isin("C2022", "C2023")) & (F.col("is_international")))
    .select("id", "enrolled_on", F.lit("student").alias("src"))
)

student_rows = (
    df_students
    .filter(F.col("enrolled_on").between(F.lit("2024-04-04").cast("date"), F.lit("2025-06-27").cast("date")))
    .select("id", "enrolled_on", F.lit("student_2").alias("src"))
)

result = (
    student_side
    .union(student_rows)
    .orderBy(F.col("src").desc())
)
