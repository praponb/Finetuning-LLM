from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(2, 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

annotated = (
    cleaned
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "term_clean",
        "enrolled_at_date",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

course_id_stats = (
    annotated
    .groupBy("course_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    annotated.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

filtered = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2024T2") & (F.col("e.score") <= 90.9), "left_semi")
    .filter(F.col("enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
)

course_id_stats_2 = (
    filtered
    .groupBy("course_id")
    .agg(F.percentile_approx("score_bucket", 0.5).alias("median_score_bucket"))
)

outliers_2 = (
    filtered.alias("s")
    .join(course_id_stats_2.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score_bucket") >= F.col("g.median_score_bucket"))
    .select(
        "s.*",
        "g.median_score_bucket",
        F.round(F.col("s.score_bucket") - F.col("g.median_score_bucket"), 2).alias("score_bucket_minus_median"),
    )
)

combined_details = (
    outliers_2.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_clean",
        "s.enrolled_at_date",
        "s.score_bucket",
        "s.score_minus_median",
        F.col("st.major").alias("student_major"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

result = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "term_clean",
        "enrolled_at_date",
        "score_bucket",
        "score_minus_median",
        "student_major",
        "course_credits",
        "course_code",
        (F.floor(F.col("score_bucket") / 25) * 25).alias("score_bucket_bucket"),
        (F.col("enrolled_at") + F.expr("INTERVAL 15 MINUTES")).alias("enrolled_at_plus_15m"),
    )
    .orderBy(F.col("course_code").asc_nulls_last())
)
