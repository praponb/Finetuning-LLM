from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("cohort") != "C2022")
    .select("id", F.col("gpa").alias("metric"), "enrolled_on")
)

student_rows = df_students.select("id", "gpa", "enrolled_on")

combined = (
    student_side
    .union(student_rows)
    .distinct()
)

result = (
    combined
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("metric").alias("total_metric"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("id")
)
