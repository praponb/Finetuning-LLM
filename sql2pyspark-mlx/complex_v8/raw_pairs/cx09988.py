from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
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

combined_details_student = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
    )
)

joined = (
    combined_details_student.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.is_international")), "left")
    .select("s.*", F.col("st.cohort").alias("student_cohort"))
)

labeled = (
    joined
    .filter(F.col("course_id") <= 10)
    .withColumn("student_gpa_adj", F.round(F.col("student_gpa") * 1.1, 2))
)

result = (
    labeled
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "student_is_international",
        "student_gpa",
        F.concat(F.col("student_major"), F.lit("#"), F.col("student_id").cast("string")).alias("student_major_ref"),
    )
    .orderBy("term", "student_gpa")
)
