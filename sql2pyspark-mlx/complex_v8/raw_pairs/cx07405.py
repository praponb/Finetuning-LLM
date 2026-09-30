from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") < 73.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

line_level_enrollments = (
    filtered_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") <= 79.2), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

combined_details = (
    line_level_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_grade_status",
        "s.enrollment_student_id",
        "s.enrollment_score",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
    )
)

stats = (
    combined_details
    .filter(F.col("enrollment_score").between(17.4, 94.7))
    .groupBy(
        F.date_format("student_enrolled_on", "yyyy-MM").alias("student_enrolled_on_ym"),
        "student_is_international",
        "student_id",
    )
    .agg(
        F.round(F.sum("score"), 2).alias("total_score"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.round(F.avg("enrollment_score"), 2).alias("avg_enrollment_score"),
        F.count(F.when(~F.col("student_is_international"), 1)).alias("n_student_is_international_fal"),
        F.max("score").alias("max_score"),
    )
)

result = (
    stats
    .select(
        "student_enrolled_on_ym",
        "avg_enrollment_score",
        "n_student_is_international_fal",
        F.abs(F.col("total_score") - 180.0).alias("total_score_distance"),
    )
    .orderBy(
        F.col("student_enrolled_on_ym").asc_nulls_last(),
        F.col("total_score_distance").asc_nulls_last(),
    )
)
