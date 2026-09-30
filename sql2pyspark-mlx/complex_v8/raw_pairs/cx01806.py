from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("gpa") < 3.4) & (~F.col("is_international")))
    .select(
        F.col("student_name").alias("category"),
        F.col("gpa").alias("amount"),
        F.col("id").alias("entity_id"),
    )
)

student_rows_2 = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 8)
    .select(
        F.col("cohort").alias("category"),
        F.col("gpa").alias("amount"),
        F.col("id").alias("entity_id"),
    )
)

result = (
    student_rows
    .unionByName(student_rows_2)
    .orderBy("entity_id")
)
