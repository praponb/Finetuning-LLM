from pyspark.sql import functions as F

prepared_students = df_students

student_side = (
    prepared_students
    .select(F.col("cohort").alias("category"), "id", F.col("gpa").alias("metric"))
)

student_side_2 = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-04-01").cast("date"))
    .select("major", "id", "gpa")
)

result = (
    student_side
    .union(student_side_2)
    .distinct()
    .orderBy(F.col("metric").desc(), "category")
)
