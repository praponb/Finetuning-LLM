from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("major").isNotNull()) & (~F.col("is_international")))
    .select(
        F.lit("student").alias("source"),
        F.col("id").alias("ref_id"),
        "major",
        "enrolled_on",
    )
)

student_side = (
    df_students
    .filter(~F.col("cohort").isin("C2021", "C2023"))
    .select(
        F.lit("student_2").alias("source"),
        F.col("id").alias("ref_id"),
        "major",
        "enrolled_on",
    )
)

result = student_rows.union(student_side)
