from pyspark.sql import functions as F

student_rows = df_students.select("id")

student_side = (
    df_students
    .filter((F.col("enrolled_on") < F.lit("2025-07-18").cast("date")) & (~F.col("cohort").isin("C2022", "C2023")))
    .select("id")
)

result = (
    student_rows
    .exceptAll(student_side)
    .orderBy(F.col("id").desc())
)
