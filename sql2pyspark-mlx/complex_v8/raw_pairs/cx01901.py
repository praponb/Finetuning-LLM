from pyspark.sql import functions as F

course_rows = df_courses.select("id", F.col("level").alias("category"))

enrollment_side = (
    df_enrollments
    .filter((F.col("score") > 16.3) & (F.col("term") != "2025T2"))
    .select(F.col("student_id").alias("id"), F.col("term").alias("category"))
)

enrollment_side_2 = (
    df_enrollments
    .select(F.col("course_id").alias("id"), F.col("term").alias("category"))
)

stacked = (
    course_rows
    .union(enrollment_side)
    .union(enrollment_side_2)
)

result = (
    stacked
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("category")
)
