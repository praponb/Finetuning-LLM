from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score") < 57.6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.when(F.col("score") >= 83.1, "A").when(F.col("score") >= 26.2, "B").otherwise("C").alias("score_tier"),
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

enriched = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.is_international")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.term_slug",
        "s.score_tier",
        "s.term_grade_status_label",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        "c.course_code",
    )
)

result = (
    enriched
    .rollup("course_id", "score_tier", "student_id")
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.min("score").alias("min_score"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("score_tier").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
