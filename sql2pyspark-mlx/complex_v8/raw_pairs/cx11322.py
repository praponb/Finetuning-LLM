from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.col("enrolled_at") >= F.lit("2025-01-13 11:00:00").cast("timestamp")) & (F.col("student_id") <= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

summary = (
    prepared
    .groupBy("student_id", "term")
    .agg(
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("score"), 2).alias("avg_score"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_details = (
    summary.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.is_international"))
    .select(
        "s.student_id",
        "s.term",
        "s.stddev_score",
        "s.n_rows",
        F.col("st.is_international").alias("student_is_international"),
    )
)

annotated = (
    with_details
    .filter(F.col("student_is_international"))
    .select(
        "*",
        (F.col("n_rows") - F.col("stddev_score")).alias("n_rows_minus_stddev_score"),
        F.abs(F.col("n_rows") - 8).alias("n_rows_distance"),
        F.when(F.col("student_is_international"), "yes").otherwise("no").alias("student_is_international_label"),
    )
)

screened = (
    annotated
    .crossJoin(annotated.agg(F.avg("n_rows_minus_stddev_score").alias("overall_avg_n_rows_minus_stddev_score")))
    .filter(F.col("n_rows_minus_stddev_score") > F.col("overall_avg_n_rows_minus_stddev_score"))
    .drop("overall_avg_n_rows_minus_stddev_score")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_stats = (
    screened.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    with_stats
    .select("stddev_score", "n_rows_distance", "student_id")
    .distinct()
    .orderBy(F.col("stddev_score").desc())
)
