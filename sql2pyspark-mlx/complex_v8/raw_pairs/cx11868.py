from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

with_details_student = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("st.cohort") == "C2023")
    .select(
        "s.*",
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_parent_course = (
    with_details_student.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") < 4), "inner")
    .filter(F.col("c.dept") != "Math")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_date",
        "s.grade_status_term_label",
        "s.student_cohort",
        "s.course_dept",
        F.col("c.dept").alias("course_dept_2"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

with_details_student_2 = (
    with_parent_course.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.enrolled_at_date",
        "s.grade_status_term_label",
        "s.student_cohort",
        "s.course_dept_2",
        "s.course_credits",
        "s.course_level",
        "st.student_name",
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.credits").alias("course_credits_2"),
    )
)

result = (
    with_details_student_2
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp"))
    .select(
        "id",
        "enrolled_at",
        "enrolled_at_date",
        "course_credits",
        "course_level",
        "student_gpa",
        "course_credits_2",
        F.when(F.col("course_dept_2").isin("CS", "Biz"), "group_a").when(F.col("course_dept_2") == "Art", "group_b").otherwise("other").alias("course_dept_2_group"),
    )
)
