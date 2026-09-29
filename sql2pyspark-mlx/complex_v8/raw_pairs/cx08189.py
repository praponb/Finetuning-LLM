from pyspark.sql import functions as F

course_side = (
    df_courses
    .select("credits", F.col("dept").alias("name"), F.lit("course").alias("source"))
)

student_rows = (
    df_students
    .filter((F.col("cohort").isin("C2022", "C2024", "C2023")) & (F.year(F.col("enrolled_on")) == 2025))
    .select(
        F.col("id").alias("credits"),
        F.col("major").alias("name"),
        F.lit("student").alias("source"),
    )
)

result = (
    course_side
    .union(student_rows)
    .distinct()
    .orderBy("source")
)
