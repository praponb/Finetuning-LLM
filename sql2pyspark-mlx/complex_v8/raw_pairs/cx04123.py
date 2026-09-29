from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
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

with_details = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

line_level_enrollments = (
    with_details.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

with_children = (
    line_level_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_grade_status",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enriched_enrollment = (
    with_children.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.score").between(8.7, 74.9)), "inner")
    .select(
        "s.*",
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
        F.col("e.student_id").alias("enrollment_student_id_2"),
        F.col("e.term").alias("enrollment_term_2"),
    )
)

enriched = (
    enriched_enrollment
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_id_2",
        "enrollment_score_2",
        "enrollment_enrolled_at",
        "enrollment_grade_status_2",
        "enrollment_student_id_2",
        "enrollment_term_2",
        F.abs(F.col("enrollment_score_2") - 57.6).alias("enrollment_score_2_distance"),
        F.when(F.col("enrollment_score_2") >= 66.0, "HIGH").when(F.col("enrollment_score_2") >= 20.6, "MEDIUM").otherwise("LOW").alias("enrollment_score_2_tier"),
        (F.floor(F.col("enrollment_score_2") / 25) * 25).alias("enrollment_score_2_bucket"),
    )
)

result = (
    enriched
    .filter(F.col("enrollment_enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp"))
    .select(
        "id",
        "course_id",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_score_2",
        "enrollment_enrolled_at",
        "enrollment_student_id_2",
        "enrollment_score_2_distance",
        "enrollment_score_2_bucket",
        F.coalesce(F.col("enrollment_score_2"), F.lit(0.0)).alias("enrollment_score_2_filled"),
    )
    .orderBy(
        F.col("id").asc_nulls_last(),
        F.col("enrollment_score_2_bucket").desc_nulls_first(),
    )
)
