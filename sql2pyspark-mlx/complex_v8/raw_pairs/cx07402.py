from pyspark.sql import functions as F

student_rows = df_students.select(F.lit("student").alias("source"), "id", "student_name")

course_side = df_courses.select(F.lit("course").alias("source"), "id", "level")

student_side = (
    df_students
    .filter((F.col("cohort") == "C2021") & (F.length(F.trim(F.col("student_name"))) >= 7))
    .select(F.lit("student_3").alias("source"), "id", "major")
)

unioned = (
    student_rows
    .union(course_side)
    .union(student_side)
)

result = (
    unioned
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("source")
)
