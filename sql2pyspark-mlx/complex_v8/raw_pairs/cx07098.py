from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("is_international"))
    .select("cohort")
)

student_side = (
    df_students
    .filter((F.col("enrolled_on") < F.lit("2025-07-03").cast("date")) & (F.col("major") == "CS"))
    .select("major")
)

result = student_rows.subtract(student_side)
