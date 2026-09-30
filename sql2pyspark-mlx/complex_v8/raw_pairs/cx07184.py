from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
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

joined_course = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    joined_course
    .cube("course_credits", "grade_status", "course_level")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("score") / F.nullif(F.sum("course_credits"), F.lit(0)), 3).alias("score_to_course_credits_ratio"),
    )
    .select(
        "course_credits",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        F.coalesce(F.col("course_level"), F.lit("ALL")).alias("course_level"),
        "gid",
        "score_to_course_credits_ratio",
    )
    .orderBy(
        F.col("course_credits").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("course_level").asc_nulls_last(),
    )
)
