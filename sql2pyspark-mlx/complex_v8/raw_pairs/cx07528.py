from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term") != "2024T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

derived = (
    filtered_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "grade_status_term_label",
        F.when(F.col("score") >= 57.6, "large").when(F.col("score") >= 32.9, "medium").otherwise("small").alias("score_tier"),
    )
)

student_id_stats = (
    derived
    .groupBy("student_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

compared = (
    derived.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

enriched = (
    compared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.grade_status_term_label",
        "s.median_score",
        "s.score_minus_median",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    enriched
    .filter(F.col("course_id") <= 4)
    .select(
        "id",
        "course_id",
        "score",
        "enrolled_at",
        "grade_status_term_label",
        "median_score",
        "score_minus_median",
        "student_gpa",
        "student_cohort",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
    .orderBy(F.col("grade_status_term_label").desc_nulls_first())
)
