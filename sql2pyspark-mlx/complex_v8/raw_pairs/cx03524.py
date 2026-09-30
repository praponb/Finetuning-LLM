from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("gpa") <= 3.4)
    .select(F.col("id").alias("key_id"), F.col("enrolled_on").alias("event_date"))
)

student_rows_2 = df_students.select("id", "enrolled_on")

result = (
    student_rows
    .union(student_rows_2)
    .orderBy("key_id")
)
