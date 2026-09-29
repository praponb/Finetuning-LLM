from pyspark.sql import functions as F

student_side = (
    df_students
    .select(F.col("id").alias("ref_id"), "gpa", F.col("enrolled_on").alias("day"))
)

student_side_2 = (
    df_students
    .select(F.col("id").alias("ref_id"), "gpa", F.col("enrolled_on").alias("day"))
)

result = (
    student_side
    .union(student_side_2)
    .distinct()
    .orderBy("gpa")
)
