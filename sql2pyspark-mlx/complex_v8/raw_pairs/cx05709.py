from pyspark.sql import functions as F

student_side = df_students.select(F.col("id").alias("ref_id"), F.col("cohort").alias("category"))

student_side_2 = df_students.select(F.col("id").alias("ref_id"), F.col("cohort").alias("category"))

result = (
    student_side
    .intersect(student_side_2)
    .orderBy(F.col("category").desc())
)
