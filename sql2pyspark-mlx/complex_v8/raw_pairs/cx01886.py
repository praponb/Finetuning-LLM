from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-03-02 08:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

joined_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at_week",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

derived = (
    joined_student
    .select(
        "id",
        "student_id",
        "student_enrolled_on",
        "course_dept",
        "course_code",
        F.date_format(F.col("enrolled_at_week"), "MMM yyyy").alias("enrolled_at_week_label"),
    )
)

combined_details_student = (
    derived.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_dept",
        "s.course_code",
        "s.enrolled_at_week_label",
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "course_code",
        "student_cohort",
    )
    .orderBy(F.col("id").asc())
)
