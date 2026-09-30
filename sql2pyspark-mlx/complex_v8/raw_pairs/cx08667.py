from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

joined_student = (
    recent.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") == "C2023"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    joined_student
    .groupBy("term", "enrolled_at")
    .pivot("grade_status", ["WITHDRAWN", "INCOMPLETE"])
    .agg(F.count("score").alias("count_v"), F.sum("score").alias("sum_v"))
    .orderBy("term", "enrolled_at")
)
