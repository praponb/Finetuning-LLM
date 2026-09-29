from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 59.7)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

enriched = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    enriched
    .groupingSets(
        [["student_is_international", "term"], ["student_is_international"]],
        "student_is_international", "term",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.min("score").alias("min_score"),
    )
    .select(
        "student_is_international",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "grouping_level",
        "n_rows",
        "first_enrolled_at",
        "min_score",
    )
    .orderBy(
        F.col("student_is_international").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
