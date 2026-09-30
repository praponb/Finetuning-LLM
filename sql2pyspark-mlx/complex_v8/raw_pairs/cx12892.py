from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

labeled = (
    prepared
    .select(
        "*",
        (F.col("enrolled_at") + F.expr("INTERVAL 7 DAYS")).alias("enrolled_at_plus_7d"),
        F.abs(F.col("score_adj") - 97.5).alias("score_adj_distance"),
    )
)

with_activity = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.hour(F.col("e.enrolled_at")).between(8, 14)), "left_semi")
)

with_parent_student = (
    with_activity.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    with_parent_student
    .groupingSets(
        [["grade_status_len", "course_id"], ["grade_status_len"], ["course_id"], []],
        "grade_status_len", "course_id",
    )
    .agg(
        F.grouping("grade_status_len").alias("is_total_grade_status_len"),
        F.sum("score_adj_distance").alias("total_score_adj_distance"),
        F.count(F.when(F.col("student_cohort") == "C2021", 1)).alias("n_student_cohort_c2021"),
        F.round(F.sum("score_adj_distance"), 2).alias("total_score_adj_distance_2"),
    )
)
