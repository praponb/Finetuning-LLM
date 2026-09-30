from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("is_international")) & (F.col("enrolled_on").between(F.lit("2024-03-15").cast("date"), F.lit("2025-05-07").cast("date"))))
    .select("major", "enrolled_on")
)

student_side_2 = df_students.select(F.col("cohort").alias("major"), "enrolled_on")

result = student_side.union(student_side_2)
