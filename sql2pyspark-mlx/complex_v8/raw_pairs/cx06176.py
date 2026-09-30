from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat_ws("|", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

enriched_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (~F.col("st.cohort").isin("C2022", "C2024")), "inner")
    .filter(~F.col("st.is_international"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_term_label",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

derived = (
    enriched_student
    .filter(F.col("grade_status_term_label").isNotNull())
    .select(
        "*",
        F.when(F.col("term").isin("2025T2", "2024T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        F.concat(F.col("grade_status_term_label"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_term_label_ref"),
        (F.col("student_gpa") - F.col("score")).alias("student_gpa_minus_score"),
    )
)

without_matches = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)) & (F.col("e.term").isNotNull()), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["course_id", "term_group", "student_is_international"], ["course_id"], ["term_group"]],
        "course_id", "term_group", "student_is_international",
    )
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("term_group").asc_nulls_last(),
        F.col("student_is_international").asc_nulls_last(),
    )
)
