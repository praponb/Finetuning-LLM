from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("gpa") > 2.1)
    .select("id", "gpa", F.lit("student").alias("record_type"))
)

student_side_2 = (
    df_students
    .filter(F.col("cohort").isin("C2021", "C2024", "C2022"))
    .select("id", "gpa", F.lit("student_2").alias("record_type"))
)

stacked = student_side.union(student_side_2)

result = (
    stacked
    .groupBy("record_type")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("record_type")
)
