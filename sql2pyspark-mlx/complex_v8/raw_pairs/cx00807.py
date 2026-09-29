from pyspark.sql import functions as F

student_rows = df_students.select(F.col("id").alias("entity_id"))

student_side = (
    df_students
    .filter(F.col("cohort") == "C2023")
    .select(F.col("id").alias("entity_id"))
)

result = (
    student_rows
    .intersectAll(student_side)
    .orderBy("entity_id")
)
