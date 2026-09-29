from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T1", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

enriched = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.term_group",
        "s.score_adj",
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

qualified = (
    enriched
    .crossJoin(enriched.agg(F.min("score_adj").alias("overall_min_score_adj")))
    .filter(F.col("score_adj") > F.col("overall_min_score_adj"))
    .drop("overall_min_score_adj")
)

result = (
    qualified
    .filter(F.col("course_dept") == "Biz")
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "course_code",
    )
    .orderBy(F.col("course_code").asc_nulls_last(), "id")
    .limit(10)
)
