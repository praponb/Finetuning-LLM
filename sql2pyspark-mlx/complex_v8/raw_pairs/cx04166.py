from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

detail_rows_enrollments = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") != "2025T1"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status_term_label",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

enriched = (
    detail_rows_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.term") == "2024T1"), "inner")
    .filter(F.col("e.term") != "2025T1")
    .select(
        "s.*",
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("e.term").alias("enrollment_term_2"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["student_id", "term"], ["student_id"]],
        "student_id", "term",
    )
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.sum(F.when(F.col("enrollment_term_2") == "2025T1", F.col("score"))).alias("score_enrollment_term_2_2025t1"),
        F.round(100 * F.count(F.when(F.col("enrollment_term_2") == "2025T1", 1)) / F.count("*"), 1).alias("pct_enrollment_term_2_2025t1"),
    )
    .select(
        "student_id",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "is_total_student_id",
        "score_enrollment_term_2_2025t1",
        "pct_enrollment_term_2_2025t1",
    )
    .orderBy(F.col("student_id").asc_nulls_last(), F.col("term").asc_nulls_last())
)
