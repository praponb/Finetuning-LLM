from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("B%"))
    .select(F.col("id").alias("qty"), "credits")
)

enrollment_rows = df_enrollments.select("course_id", "student_id")

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id") <= 10) & (F.col("term").isin("2024T1", "2025T1")))
    .select("id", "student_id")
)

all_events = (
    course_rows
    .union(enrollment_rows)
    .union(enrollment_side)
)

result = (
    all_events
    .groupBy("qty")
    .agg(F.count("*").alias("n_rows"), F.sum("credits").alias("total_credits"))
    .orderBy("qty")
)
