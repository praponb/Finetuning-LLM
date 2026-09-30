from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

combined_details_course = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

student_id_stats = (
    combined_details_course
    .groupBy("student_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    combined_details_course.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

enriched = (
    outliers.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_grade_status_label",
        "s.score_filled",
        "s.course_dept",
        "s.course_level",
        "s.course_code",
        "s.student_enrolled_on",
        "s.student_cohort",
        "s.median_score",
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    enriched
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        "score_filled",
        "course_dept",
        "course_level",
        "student_enrolled_on",
        "median_score",
        "student_major",
        "student_gpa",
    )
    .orderBy("student_enrolled_on", F.col("student_major").desc(), "id")
    .limit(20)
)
