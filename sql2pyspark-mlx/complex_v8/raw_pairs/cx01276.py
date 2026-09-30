from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter(F.col("score") > 38.1)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("|", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

with_counts = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

enriched_student = (
    with_counts.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.avg_enrollment_score",
        F.col("st.major").alias("student_major"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

tagged = (
    enriched_student
    .select(
        "*",
        (F.col("course_credits") - F.col("avg_enrollment_score")).alias("course_credits_minus_avg_enrollment_score"),
    )
)

scored = (
    tagged
    .filter(F.col("course_credits").between(2, 3))
    .select(
        "*",
        F.concat_ws(": ", F.col("course_code"), F.col("student_major")).alias("course_code_student_major_label"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count(F.when(F.col("term") == "2024T2", 1)).alias("enrollment_term_count"),
    )
)

profiled = (
    scored.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["enrollment_term_count", "student_is_international", "course_credits"], ["enrollment_term_count"], []],
        "enrollment_term_count", "student_is_international", "course_credits",
    )
    .agg(F.min("course_credits").alias("min_course_credits"))
)
