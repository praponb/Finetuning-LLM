from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("course_id", F.col("score").alias("amount"))

student_rows = df_students.select("id", "gpa")

unioned = enrollment_side.union(student_rows)

result = (
    unioned
    .groupBy("course_id")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("course_id")
)
