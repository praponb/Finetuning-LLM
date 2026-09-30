from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("is_international"))
    .select("student_name", "enrolled_on")
)

student_side = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("J%")) & (F.col("cohort") == "C2023"))
    .select(F.col("major").alias("student_name"), "enrolled_on")
)

result = student_rows.union(student_side)
