from pyspark.sql import functions as F

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

joined_student = (
    joined_ranges.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_7d",
        "s.prior_score_7d",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

with_parent = (
    joined_student.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.enrolled_on") >= F.lit("2024-03-15").cast("date")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_7d",
        "s.prior_score_7d",
        "s.student_enrolled_on",
        F.col("st.cohort").alias("student_cohort"),
    )
)

student_id_stats = (
    with_parent
    .groupBy("student_id")
    .agg(F.percentile_approx("prior_rows_7d", 0.5).alias("median_prior_rows_7d"))
)

outliers = (
    with_parent.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_rows_7d") >= F.col("g.median_prior_rows_7d"))
    .select(
        "s.*",
        "g.median_prior_rows_7d",
        F.round(F.col("s.prior_rows_7d") - F.col("g.median_prior_rows_7d"), 2).alias("prior_rows_7d_minus_median"),
    )
)

result = (
    outliers
    .select(
        "id",
        "prior_rows_7d",
        "prior_score_7d",
        "student_enrolled_on",
        "student_cohort",
        "median_prior_rows_7d",
        "prior_rows_7d_minus_median",
        (F.col("enrolled_at") + F.expr("INTERVAL 2 HOURS")).alias("enrolled_at_plus_2h"),
    )
)
