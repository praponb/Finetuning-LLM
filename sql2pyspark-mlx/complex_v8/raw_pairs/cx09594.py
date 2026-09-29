from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("gpa") < 3.4) & (F.upper(F.trim(F.col("student_name"))).like("H%")))
    .select("id", F.col("gpa").alias("amount"))
)

enrollment_side = (
    df_enrollments
    .filter(F.col("student_id") >= 11)
    .select("course_id", "score")
)

all_events = student_rows.union(enrollment_side)

result = (
    all_events
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("id")
)
