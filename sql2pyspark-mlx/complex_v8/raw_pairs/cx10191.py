from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") <= 57.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 39.4).alias("score_distance"),
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

joined = (
    cleaned_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
    )
)

joined_student = (
    joined.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") == "C2021")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
        "st.student_name",
    )
)

result = (
    joined_student
    .rollup("term", "student_is_international")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
    )
)
