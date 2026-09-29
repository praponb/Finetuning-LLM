from pyspark.sql import functions as F

course_side = df_courses.select(F.lit("course").alias("src"), "credits", "id")

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") >= 10.8) & (F.col("student_id").isNotNull()))
    .select(F.lit("enrollment").alias("src"), "id", "student_id")
)

unioned = course_side.union(enrollment_rows)

result = (
    unioned
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("src")
)
