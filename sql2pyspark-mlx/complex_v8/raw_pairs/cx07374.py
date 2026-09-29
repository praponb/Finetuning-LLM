from pyspark.sql import functions as F

in_window = (
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

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

with_stats = (
    in_window.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
        F.count(F.when(F.col("term").isin("2024T1", "2024T2", "2025T1"), 1)).alias("enrollment_term_count"),
    )
)

summarized_rows = (
    with_stats.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

student_id_stats = (
    summarized_rows
    .groupBy("student_id")
    .agg(F.avg("prior_score_7d").alias("avg_prior_score_7d"))
)

vs_group = (
    summarized_rows.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_score_7d") > F.col("g.avg_prior_score_7d"))
    .select(
        "s.*",
        "g.avg_prior_score_7d",
        F.round(F.col("s.prior_score_7d") - F.col("g.avg_prior_score_7d"), 2).alias("prior_score_7d_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_7d",
        "prior_score_7d",
        "first_enrollment_enrolled_at",
        "last_enrollment_enrolled_at",
        "first_enrollment_enrolled_at_2",
        "enrollment_term_count",
        "avg_prior_score_7d",
        "prior_score_7d_minus_avg",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
    .orderBy(F.col("student_id").desc_nulls_first(), F.col("enrolled_at").desc(), "id")
    .limit(10)
)
