from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("gpa") < 3.4) & (F.col("is_international")))
    .select(F.col("id").alias("key_id"), "enrolled_on", "gpa")
)

student_side_2 = (
    df_students
    .filter(F.year(F.col("enrolled_on")) == 2024)
    .select("id", "enrolled_on", "gpa")
)

result = (
    student_side
    .union(student_side_2)
    .orderBy(F.col("enrolled_on").desc(), "key_id")
)
