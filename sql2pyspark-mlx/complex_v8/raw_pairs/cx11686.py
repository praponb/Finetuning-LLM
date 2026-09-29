from pyspark.sql import functions as F

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

with_parent_student = (
    base.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
    )
)

with_parent_student_2 = (
    with_parent_student.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort") == "C2023"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.student_enrolled_on",
        "s.student_name",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.student_name").alias("student_student_name"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.dept").alias("course_dept"),
    )
)

tagged = (
    with_parent_student_2
    .filter(F.lower(F.col("student_name")).like("%n %"))
    .select(
        "*",
        F.when(F.col("student_gpa") >= 3.2, "A").when(F.col("student_gpa") >= 2.5, "B").otherwise("C").alias("student_gpa_tier"),
        F.round(F.col("score") / F.nullif(F.col("student_gpa"), F.lit(0)), 3).alias("score_per_student_gpa"),
    )
)

result = (
    tagged
    .select(
        "id",
        "score",
        "student_enrolled_on",
        "student_name",
        "student_gpa",
        "student_student_name",
        "student_cohort",
        "student_gpa_tier",
        "score_per_student_gpa",
        F.lower(F.trim(F.col("student_student_name"))).alias("student_student_name_clean"),
        F.least(F.col("score"), F.col("student_gpa")).alias("least_score_student_gpa"),
    )
    .orderBy(
        F.col("student_enrolled_on").asc_nulls_last(),
        F.col("student_name").desc(),
    )
)
