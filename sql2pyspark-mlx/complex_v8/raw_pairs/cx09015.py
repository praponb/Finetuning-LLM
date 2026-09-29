from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(~F.col("grade_status").isin("INCOMPLETE", "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (~F.col("e.term").isin("2024T1", "2024T2")) & (F.col("e.score") >= 42.4), "left_semi")
)

joined_course = (
    screened.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_day",
        F.col("c.level").alias("course_level"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    joined_course
    .groupingSets(
        [["course_level", "course_id"], ["course_level"]],
        "course_level", "course_id",
    )
    .agg(
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.sum(F.when(F.col("course_level").isNotNull(), F.col("score")).otherwise(0)).alias("score_course_level_is_not_null"),
    )
    .orderBy(
        F.col("course_level").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
