from pyspark.sql import functions as F

course_rows = df_courses.select("credits", F.col("course_code").alias("category"), "id")

enrollment_side = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select("id", "grade_status", "student_id")
)

all_events = (
    course_rows
    .union(enrollment_side)
    .distinct()
)

result = (
    all_events
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("category")
)
