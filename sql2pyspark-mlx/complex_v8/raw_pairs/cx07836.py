from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("course_id") <= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month_2"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_details_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_month_2",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_details_student
    .groupBy("student_enrolled_on", "student_id")
    .pivot("grade_status", ["FAIL", "WITHDRAWN", "PASS", "INCOMPLETE"])
    .agg(F.avg("score").alias("avg_v"), F.max("score").alias("max_v"))
    .orderBy("student_enrolled_on", "student_id")
)
