from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 13:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 54.2).alias("score_distance"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

screened = (
    valid_enrollments
    .crossJoin(valid_enrollments.agg(F.max("score_distance").alias("overall_max_score_distance")))
    .filter(F.col("score_distance") < F.col("overall_max_score_distance"))
    .drop("overall_max_score_distance")
)

grade_status_stats = (
    screened
    .groupBy("grade_status")
    .agg(F.avg("score_distance").alias("avg_score_distance"))
)

outliers = (
    screened.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score_distance") > F.col("g.avg_score_distance"))
    .select(
        "s.*",
        "g.avg_score_distance",
        F.round(F.col("s.score_distance") - F.col("g.avg_score_distance"), 2).alias("score_distance_minus_avg"),
    )
)

with_parent = (
    outliers.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa") >= 2.7), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("st.major") == "CS")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_distance",
        "s.term_or_unknown",
        "s.avg_score_distance",
        "s.score_distance_minus_avg",
        F.col("st.major").alias("student_major"),
        F.col("st.gpa").alias("student_gpa"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "student_id",
        "term_or_unknown",
        "avg_score_distance",
        "score_distance_minus_avg",
        "student_major",
        "student_gpa",
        "course_credits",
        F.round(F.col("score_distance") / F.nullif(F.col("avg_score_distance"), F.lit(0)), 3).alias("score_distance_per_avg_score_distance"),
        F.when(F.col("score_distance_minus_avg") >= 20.0, "HIGH").when(F.col("score_distance_minus_avg") >= -40.0, "MEDIUM").otherwise("LOW").alias("score_distance_minus_avg_tier"),
    )
    .orderBy(
        F.col("student_gpa").desc_nulls_first(),
        F.col("avg_score_distance").desc_nulls_first(),
    )
)
