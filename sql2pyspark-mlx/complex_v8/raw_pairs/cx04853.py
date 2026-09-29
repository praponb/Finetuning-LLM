from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("grade_status").isin("PASS", "INCOMPLETE", "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd HH:00").alias("enrolled_at_label"),
        F.when(F.col("score") >= 79.2, "large").when(F.col("score") >= 13.9, "medium").otherwise("small").alias("score_tier"),
    )
)

combined_details_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select("s.*", F.col("st.major").alias("student_major"), "st.student_name")
)

annotated = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "grade_status_or_unknown",
        "enrolled_at_label",
        "score_tier",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label_2"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
    )
)

exploded_detail = (
    annotated.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") >= 30.0), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.grade_status",
        "s.grade_status_or_unknown",
        "s.enrolled_at_label",
        "s.score_tier",
        "s.enrolled_at_plus_30m",
        "s.score_adj",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    exploded_detail
    .rollup("student_id", "enrollment_term", "grade_status")
    .agg(F.round(F.sum("enrollment_score"), 2).alias("total_enrollment_score"))
    .select(
        "student_id",
        F.coalesce(F.col("enrollment_term"), F.lit("ALL")).alias("enrollment_term"),
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "total_enrollment_score",
    )
)
