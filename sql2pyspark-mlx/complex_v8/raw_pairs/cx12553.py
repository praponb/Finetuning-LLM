from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.col("major") == "Math")
    .select("id", F.col("gpa").alias("amount"))
)

enrollment_rows = df_enrollments.select("course_id", "score")

all_events = (
    student_side
    .union(enrollment_rows)
    .distinct()
)

result = (
    all_events
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("amount").alias("total_amount"))
    .orderBy("id")
)
