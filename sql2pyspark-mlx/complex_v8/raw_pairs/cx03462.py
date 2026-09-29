from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter(F.col("grade_status") != "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

enrollment_side = filtered.select("grade_status", F.col("student_id").alias("qty"))

course_side = df_courses.select("dept", "credits")

all_events = enrollment_side.union(course_side)

result = (
    all_events
    .groupBy("grade_status")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("grade_status")
)
