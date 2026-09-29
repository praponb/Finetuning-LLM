from pyspark.sql import functions as F

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

enriched_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_plus_15m",
        "s.term_ref",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

enriched = (
    enriched_course
    .filter(F.col("course_dept").isNotNull())
    .select("*", F.abs(F.col("score") - 35.8).alias("score_distance"))
)

detail_rows_enrollments = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") >= 7), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_plus_15m",
        "s.term_ref",
        "s.course_level",
        "s.course_dept",
        "s.course_code",
        "s.score_distance",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

stats = (
    detail_rows_enrollments
    .groupBy(
        "grade_status",
        "enrollment_student_id",
        F.year("enrolled_at_plus_15m").alias("enrolled_at_plus_15m_year"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("enrollment_score"), 2).alias("total_enrollment_score"),
        F.sum("enrollment_score").alias("total_enrollment_score_2"),
    )
)

joined_student = (
    stats.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.enrollment_student_id"), "left")
    .select(
        "s.grade_status",
        "s.enrollment_student_id",
        "s.enrolled_at_plus_15m_year",
        "s.row_count",
        "s.total_enrollment_score_2",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    joined_student
    .select(
        "grade_status",
        "enrollment_student_id",
        "enrolled_at_plus_15m_year",
        "row_count",
        "total_enrollment_score_2",
        "student_gpa",
        "student_cohort",
        (F.col("student_gpa") + F.col("total_enrollment_score_2")).alias("student_gpa_plus_total_enrollment_score_2"),
    )
    .orderBy(F.col("student_gpa").asc())
)
