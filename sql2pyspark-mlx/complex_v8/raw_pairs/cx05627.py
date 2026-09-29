from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

with_parent_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at_label",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    with_parent_student
    .groupBy("student_id", "student_cohort")
    .pivot("grade_status", ["FAIL", "INCOMPLETE"])
    .agg(F.count("student_gpa").alias("count_v"), F.max("student_gpa").alias("max_v"))
    .orderBy("student_id", "student_cohort")
)
