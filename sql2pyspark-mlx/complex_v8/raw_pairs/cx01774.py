from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter(F.col("major").isin("Art", "Biz"))
    .select(F.col("cohort").alias("label"), F.col("gpa").alias("amount"))
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select("term", "score")
)

result = (
    student_rows
    .union(enrollment_rows)
    .orderBy("amount", "label")
)
