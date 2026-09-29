from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        F.lit("enrollment").alias("origin"),
        "student_id",
        F.col("score").alias("metric"),
    )
)

student_rows = (
    df_students
    .select(
        F.lit("student").alias("origin"),
        F.col("id").alias("student_id"),
        F.col("gpa").alias("metric"),
    )
)

result = (
    enrollment_rows
    .unionByName(student_rows)
    .orderBy("origin")
)
