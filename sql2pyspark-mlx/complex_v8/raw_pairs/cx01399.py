from pyspark.sql import functions as F

student_rows = df_students.select("student_name", "gpa")

student_rows_2 = (
    df_students
    .filter((F.col("gpa").between(1.7, 3.4)) & (F.col("is_international")))
    .select("student_name", "gpa")
)

all_events = student_rows.union(student_rows_2)

result = (
    all_events
    .groupBy("student_name")
    .agg(F.count("*").alias("n_rows"), F.sum("gpa").alias("total_gpa"))
    .orderBy("student_name")
)
