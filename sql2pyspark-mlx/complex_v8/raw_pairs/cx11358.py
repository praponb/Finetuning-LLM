from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj_2"),
    )
)

with_details = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_adj",
        "s.score_adj_2",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

with_details_2 = (
    with_details.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.course_code").isNotNull()), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.score_adj_2",
        "s.student_cohort",
        "s.student_is_international",
        "s.student_name",
        "s.course_level",
        F.col("c.level").alias("course_level_2"),
        F.col("c.credits").alias("course_credits_2"),
        F.col("st.cohort").alias("student_cohort_2"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_details_2
    .filter(F.col("student_is_international"))
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "score_adj_2",
        "student_cohort",
        "student_name",
        "course_level_2",
        "course_credits_2",
        "student_enrolled_on",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
    .orderBy(F.col("score_adj_2").asc_nulls_last(), F.col("student_name").desc())
)
