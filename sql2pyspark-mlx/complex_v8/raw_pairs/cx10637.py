from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        F.col("course_id").alias("qty"),
        F.col("score").alias("amount"),
        F.lit("enrollment").alias("origin"),
    )
)

student_rows = (
    df_students
    .select(
        F.col("id").alias("qty"),
        F.col("gpa").alias("amount"),
        F.lit("student").alias("origin"),
    )
)

unioned = enrollment_rows.union(student_rows)

result = (
    unioned
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("origin")
)
