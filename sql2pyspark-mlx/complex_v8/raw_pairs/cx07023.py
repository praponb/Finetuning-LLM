from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("cohort").isin("C2022", "C2023")) & (~F.col("is_international")))
    .select("id", F.col("gpa").alias("amount"))
)

enrollment_side = df_enrollments.select("student_id", "score")

stacked = student_side.union(enrollment_side)

result = (
    stacked
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("id")
)
