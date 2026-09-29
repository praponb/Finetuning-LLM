from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("gpa") <= 3.4) & (F.col("cohort").isNotNull()))
    .select("id", "cohort")
)

student_rows_2 = df_students.select("id", F.col("student_name").alias("cohort"))

result = student_rows.intersect(student_rows_2)
