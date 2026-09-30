from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.hour(F.col("enrolled_at")).between(9, 15))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

screened = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort").isNotNull()) & (F.col("st.gpa") > 2.0), "left_anti")
)

labeled = (
    screened
    .filter(F.col("grade_status_term_label").isNotNull())
    .select(
        "*",
        F.concat_ws("|", F.col("grade_status"), F.col("grade_status_term_label")).alias("grade_status_grade_status_term_label_label"),
        F.date_trunc("hour", F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

joined = (
    labeled.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.grade_status_term_label",
        "s.grade_status_grade_status_term_label_label",
        "s.enrolled_at_hour",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.major").alias("student_major"),
        "st.student_name",
        F.col("c.credits").alias("course_credits"),
    )
)

labeled_2 = (
    joined
    .filter(F.col("grade_status_grade_status_term_label_label").isNotNull())
    .select(
        "*",
        F.when(F.col("course_credits") >= 4, "A").when(F.col("course_credits") >= 2, "B").otherwise("C").alias("course_credits_tier"),
        F.when(F.col("course_credits") >= 4, "A").when(F.col("course_credits") >= 2, "B").otherwise("C").alias("course_credits_tier_2"),
    )
)

with_details_student = (
    labeled_2.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") != "C2021"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_term_label",
        "s.grade_status_grade_status_term_label_label",
        "s.enrolled_at_hour",
        "s.student_cohort",
        "s.student_major",
        "s.student_name",
        "s.course_credits_tier_2",
        F.col("st.major").alias("student_major_2"),
        F.col("st.student_name").alias("student_student_name"),
        F.col("st.gpa").alias("student_gpa"),
        "c.course_code",
        F.col("c.dept").alias("course_dept"),
    )
)

result = (
    with_details_student
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status_term_label",
        "enrolled_at_hour",
        "student_name",
        "course_credits_tier_2",
        "student_student_name",
        "student_gpa",
        "course_dept",
        F.coalesce(F.col("student_major_2"), F.lit("UNKNOWN")).alias("student_major_2_or_unknown"),
        F.round(F.col("student_gpa") * 1.07, 2).alias("student_gpa_adj"),
    )
    .orderBy(F.col("student_gpa").desc_nulls_first(), "course_dept")
)
