from pyspark.sql import functions as F

valid = (
    df_courses
    .filter(F.col("level") == "advanced")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
    )
)

line_level = (
    valid.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.course_code",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

per_group = (
    line_level
    .groupBy(
        "enrollment_student_id",
        F.date_trunc("month", "enrollment_enrolled_at").alias("enrollment_enrolled_at_month"),
    )
    .agg(
        F.round(F.avg("enrollment_score"), 2).alias("avg_enrollment_score"),
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("course_code"))) >= 8, 1)) / F.count("*"), 1).alias("pct_length_trim_course_code_8"),
        F.sum("enrollment_score").alias("total_enrollment_score"),
    )
)

exploded_detail = (
    per_group.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.enrollment_student_id"), "inner")
    .select(
        "s.enrollment_student_id",
        "s.enrollment_enrolled_at_month",
        "s.total_enrollment_score",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

combined_details = (
    exploded_detail.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.enrollment_student_id"), "inner")
    .select(
        "s.enrollment_student_id",
        "s.enrollment_enrolled_at_month",
        "s.total_enrollment_score",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_course_id",
        "s.enrollment_enrolled_at",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    combined_details
    .cube("enrollment_course_id", "enrollment_student_id", "student_cohort")
    .agg(
        F.grouping("enrollment_course_id").alias("is_total_enrollment_course_id"),
        F.count(F.when(F.col("enrollment_term").isNotNull(), 1)).alias("n_enrollment_term_is_not_null"),
        F.round(F.sum("student_gpa"), 2).alias("total_student_gpa"),
        F.min("enrollment_enrolled_at_month").alias("first_enrollment_enrolled_at_month"),
    )
    .select(
        "enrollment_course_id",
        "enrollment_student_id",
        F.coalesce(F.col("student_cohort"), F.lit("ALL")).alias("student_cohort"),
        "is_total_enrollment_course_id",
        "n_enrollment_term_is_not_null",
        "total_student_gpa",
        "first_enrollment_enrolled_at_month",
    )
    .orderBy(
        F.col("enrollment_course_id").asc_nulls_last(),
        F.col("enrollment_student_id").asc_nulls_last(),
        F.col("student_cohort").asc_nulls_last(),
    )
)
