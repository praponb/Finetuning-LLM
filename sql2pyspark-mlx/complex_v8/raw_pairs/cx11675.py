from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

exploded_detail_enrollments = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_filled",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

with_children_enrollments = (
    exploded_detail_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.score_filled",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_enrolled_at",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
    )
)

result = (
    with_children_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        "score_filled",
        "enrollment_id",
        "enrollment_term",
        "enrollment_enrolled_at",
        "enrollment_id_2",
        "enrollment_enrolled_at_2",
        F.concat_ws("|", F.col("term"), F.col("enrollment_term")).alias("term_enrollment_term_label"),
        F.when(F.col("score_filled") >= 60.0, "large").when(F.col("score_filled") >= 20.0, "medium").otherwise("small").alias("score_filled_tier"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("term").desc(),
        "enrollment_id",
        "enrollment_id_2",
    )
    .limit(10)
)
