from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 62.5, "large").when(F.col("score") >= 36.6, "medium").otherwise("small").alias("score_tier"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

line_level = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_tier",
        "s.term_or_unknown",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enriched = (
    line_level.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_tier",
        "s.term_or_unknown",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_student_id",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
    )
)

result = (
    enriched
    .groupingSets(
        [["score_tier", "student_major"], ["score_tier"]],
        "score_tier", "student_major",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
    .select(
        F.coalesce(F.col("score_tier"), F.lit("ALL")).alias("score_tier"),
        F.coalesce(F.col("student_major"), F.lit("ALL")).alias("student_major"),
        "grouping_level",
        "stddev_score",
    )
)
