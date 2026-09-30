from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("cohort") == "C2024") & (F.col("enrolled_on") < F.lit("2025-05-07").cast("date")))
    .select("id", "major")
)

student_rows_2 = (
    df_students
    .filter((F.col("gpa") < 3.4) & (F.col("enrolled_on").isNotNull()))
    .select("id", "major")
)

result = student_rows.subtract(student_rows_2)
