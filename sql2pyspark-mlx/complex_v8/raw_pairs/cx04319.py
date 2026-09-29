from pyspark.sql import functions as F

student_rows = df_students.select(F.col("student_name").alias("label"), "enrolled_on")

student_side = df_students.select(F.col("cohort").alias("label"), "enrolled_on")

result = (
    student_rows
    .unionByName(student_side)
    .distinct()
    .orderBy(F.col("label").desc(), "enrolled_on")
)
