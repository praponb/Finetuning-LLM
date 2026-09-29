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
    )
)

combined_details = (
    valid.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

tagged = combined_details.withColumn("score_distance", F.abs(F.col("score") - 49.0))

enriched = (
    tagged
    .select(
        "*",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

scored = (
    enriched
    .withColumn("student_enrolled_on_plus_90d", F.date_add(F.col("student_enrolled_on"), 90))
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("score").alias("max_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.max_enrollment_score",
        "e.first_enrollment_enrolled_at",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "student_is_international",
        "student_enrolled_on",
        "student_gpa",
        "grade_status_clean",
        "term_prefix",
        "student_enrolled_on_plus_90d",
        "enrollment_count",
        "max_enrollment_score",
        "first_enrollment_enrolled_at",
    )
    .orderBy(F.col("first_enrollment_enrolled_at").desc(), "id")
    .limit(5)
)
