from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
    )
)

with_parent_student = (
    prepared.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa") < 3.4), "inner")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

joined = (
    with_parent_student.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.upper(F.trim(F.col("st.student_name"))).like("H%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.student_is_international",
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    joined
    .select(
        "id",
        "term",
        "score",
        "student_is_international",
        "student_name",
        "student_enrolled_on",
        F.date_trunc("week", F.col("student_enrolled_on")).alias("student_enrolled_on_week"),
    )
    .orderBy("term", "student_name")
)
