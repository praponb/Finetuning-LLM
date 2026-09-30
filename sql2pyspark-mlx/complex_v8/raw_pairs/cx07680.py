from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.col("student_id") <= 18) | (F.hour(F.col("enrolled_at")).between(10, 14)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.when(F.col("score") >= 73.0, "HIGH").when(F.col("score") >= 38.5, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_parent_course = (
    recent.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_tier",
        "s.score_filled",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        "st.student_name",
        F.col("st.major").alias("student_major"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("score").alias("min_enrollment_score"))
)

with_counts = (
    with_parent_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.min_enrollment_score")
)

result = (
    with_counts
    .rollup("course_dept", "term")
    .agg(
        F.sum(F.when(F.col("term") == "2024T1", F.col("course_credits")).otherwise(0)).alias("course_credits_term_2024t1"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
    )
)
