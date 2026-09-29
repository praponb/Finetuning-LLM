from pyspark.sql import functions as F

student_rows = (
    df_students
    .select(
        F.col("id").alias("ref_id"),
        F.col("cohort").alias("category"),
        "enrolled_on",
    )
)

student_rows_2 = (
    df_students
    .filter(F.col("gpa").between(2.0, 3.4))
    .select(
        F.col("id").alias("ref_id"),
        F.col("cohort").alias("category"),
        "enrolled_on",
    )
)

stacked = student_rows.unionByName(student_rows_2)

result = (
    stacked
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("category")
)
