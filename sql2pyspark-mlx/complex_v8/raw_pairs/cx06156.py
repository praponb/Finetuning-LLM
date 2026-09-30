from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 7) | (~F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label_2"),
    )
)

enriched = (
    filtered_enrollments
    .select(
        "*",
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix_2"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 77.0, "A").when(F.col("score") >= 39.4, "B").otherwise("C").alias("score_tier"),
    )
)

with_details_student = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.term_prefix",
        "s.term_grade_status_label",
        "s.term_grade_status_label_2",
        "s.term_prefix_2",
        F.col("st.cohort").alias("student_cohort"),
    )
)

without_matches = (
    with_details_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.hour(F.col("e.enrolled_at")).between(10, 16)), "left_semi")
    .filter(F.col("student_id").isNotNull())
)

result = (
    without_matches
    .filter(F.col("term_prefix").isNotNull())
    .select(
        "id",
        "student_id",
        "grade_status",
        "term_prefix",
        "term_grade_status_label",
        "term_grade_status_label_2",
        "term_prefix_2",
        "student_cohort",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
    .orderBy(F.col("term_prefix_2").desc(), "id")
    .limit(5)
)
