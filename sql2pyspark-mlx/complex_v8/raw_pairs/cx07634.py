from pyspark.sql import functions as F

student_side = df_students.select(F.col("id").alias("qty"), "enrolled_on")

student_side_2 = df_students.select("id", "enrolled_on")

result = (
    student_side
    .union(student_side_2)
    .distinct()
)
