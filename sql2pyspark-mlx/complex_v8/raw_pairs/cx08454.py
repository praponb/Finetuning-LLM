from pyspark.sql import functions as F

course_side = df_courses.select("credits", "course_code", F.lit("course").alias("source"))

enrollment_side = (
    df_enrollments
    .filter(F.col("course_id") >= 10)
    .select(
        F.col("student_id").alias("credits"),
        F.col("grade_status").alias("course_code"),
        F.lit("enrollment").alias("source"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("student_id") >= 11) & (F.col("grade_status") == "PASS"))
    .select(
        F.col("id").alias("credits"),
        F.col("grade_status").alias("course_code"),
        F.lit("enrollment").alias("source"),
    )
)

all_events = (
    course_side
    .unionByName(enrollment_side)
    .unionByName(enrollment_rows)
)

result = (
    all_events
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("credits").alias("total_credits"))
    .orderBy("source")
)
