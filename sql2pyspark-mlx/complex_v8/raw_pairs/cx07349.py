from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.col("credits") >= 2)
    .select(F.col("id").alias("qty"), F.col("dept").alias("category"))
)

student_side = (
    df_students
    .filter(F.col("enrolled_on").between(F.lit("2024-01-24").cast("date"), F.lit("2025-07-03").cast("date")))
    .select("id", "student_name")
)

result = course_rows.intersectAll(student_side)
