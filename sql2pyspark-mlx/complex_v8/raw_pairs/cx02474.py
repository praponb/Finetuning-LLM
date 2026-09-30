from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")) & (F.col("grade_status") == "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

labeled = (
    recent_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

combined_details_student = (
    labeled.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.is_international")), "inner")
    .filter(F.upper(F.trim(F.col("st.student_name"))).like("J%"))
    .select(
        "s.*",
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

profiled = (
    combined_details_student.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
    )
)

result = (
    profiled
    .cube("grade_status_len", "course_id", "student_cohort")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("student_name").isNotNull(), F.col("score")).otherwise(0)).alias("score_student_name_is_not_null"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
        F.min("score_filled").alias("min_score_filled"),
    )
    .orderBy(
        F.col("grade_status_len").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("student_cohort").asc_nulls_last(),
    )
)
