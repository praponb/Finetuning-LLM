from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(F.lit("enrollment").alias("src"), "term", F.col("score").alias("metric"))
)

enrollment_rows_2 = df_enrollments.select(F.lit("enrollment_2").alias("src"), "grade_status", "score")

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(F.lit("enrollment_3").alias("src"), "term", "score")
)

result = (
    enrollment_rows
    .union(enrollment_rows_2)
    .union(enrollment_side)
    .orderBy(F.col("metric").desc(), "term")
)
