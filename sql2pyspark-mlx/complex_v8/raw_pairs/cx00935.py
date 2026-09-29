from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 57.6, "large").when(F.col("score") >= 36.6, "medium").otherwise("small").alias("score_tier"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 71.0, "HIGH").when(F.col("score") >= 32.9, "MEDIUM").otherwise("LOW").alias("score_tier_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    valid_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_parent_student = (
    with_stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.score_tier",
        "s.score_bucket",
        "s.score_tier_2",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

with_activity = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2025T2"), "left_semi")
    .filter(F.col("student_id") <= 18)
)

result = (
    with_activity
    .filter(F.col("score_bucket") <= 80)
    .select(
        "id",
        "student_id",
        "course_id",
        "score_tier_2",
        "student_enrolled_on",
    )
    .orderBy(F.col("course_id").desc(), "student_enrolled_on")
)
