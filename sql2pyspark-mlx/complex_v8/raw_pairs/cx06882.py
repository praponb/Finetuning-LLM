from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("term").isin("2025T1", "2025T2")) & (F.col("student_id") >= 6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

labeled = (
    scoped
    .filter(F.col("student_id").isNotNull())
    .select(
        "*",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status_clean")).alias("term_grade_status_clean_label"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.lower(F.trim(F.col("grade_status_clean"))).alias("grade_status_clean_clean"),
    )
)

tagged = (
    labeled
    .filter(F.col("term_or_unknown") == "UNKNOWN")
    .select(
        "*",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

term_or_unknown_stats = (
    tagged
    .groupBy("term_or_unknown")
    .agg(F.max("score_filled").alias("max_score_filled"))
)

outliers = (
    tagged.alias("s")
    .join(term_or_unknown_stats.alias("g"), F.col("g.term_or_unknown") == F.col("s.term_or_unknown"), "inner")
    .filter(F.col("s.score_filled") == F.col("g.max_score_filled"))
    .select(
        "s.*",
        "g.max_score_filled",
        F.round(F.col("s.score_filled") - F.col("g.max_score_filled"), 2).alias("score_filled_minus_max"),
    )
)

with_activity = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2025T2"), "left_anti")
)

per_group = (
    with_activity
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "term_or_unknown",
    )
    .agg(
        F.round(F.sum("score_filled") / F.nullif(F.sum("days_since_enrolled_at"), F.lit(0)), 3).alias("score_filled_to_days_since_enrolled_at_ratio"),
        F.round(F.avg("days_since_enrolled_at"), 2).alias("avg_days_since_enrolled_at"),
        F.max("enrolled_at").alias("last_enrolled_at"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.sum(F.when(~F.col("term").isin("2024T2", "2024T1"), 1).otherwise(0)).alias("n_term_not_in_2024t2_2024t1"),
        F.countDistinct("term").alias("distinct_terms"),
        F.max("score_filled").alias("max_score_filled_check"),
    )
    .filter(F.col("max_score_filled_check") > 10.0)
    .drop("max_score_filled_check")
)

result = (
    per_group
    .select(
        "enrolled_at_month",
        "term_or_unknown",
        "score_filled_to_days_since_enrolled_at_ratio",
        "avg_days_since_enrolled_at",
        "last_enrolled_at",
        "first_enrolled_at",
        "n_term_not_in_2024t2_2024t1",
        "distinct_terms",
    )
    .orderBy(
        F.col("n_term_not_in_2024t2_2024t1").desc(),
        "enrolled_at_month",
        "term_or_unknown",
    )
    .limit(5)
)
