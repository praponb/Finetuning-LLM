from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("dept").isin("Math", "Biz")))
    .select("credits", F.col("dept").alias("category"))
)

student_side = (
    df_students
    .filter((F.col("is_international")) & (~F.col("cohort").isin("C2022", "C2024")))
    .select(F.col("id").alias("credits"), F.col("cohort").alias("category"))
)

result = (
    course_side
    .intersect(student_side)
    .orderBy(F.col("credits").desc())
)
