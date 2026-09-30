from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("score") <= 78.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

summary = (
    scoped
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "student_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("term") == "2024T2", 1)) / F.count("*"), 1).alias("pct_term_2024t2"),
        F.sum("days_since_enrolled_at").alias("total_days_since_enrolled_at"),
        F.round(F.avg("days_since_enrolled_at"), 2).alias("avg_days_since_enrolled_at"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.max("score").alias("max_score_check"),
    )
    .filter(F.col("max_score_check") > 5.7)
    .drop("max_score_check")
)

without_matches = (
    summary.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") < 62.5) & (F.col("e.grade_status").isNotNull()), "left_anti")
)

result = (
    without_matches
    .select(
        "enrolled_at_month",
        "student_id",
        "total_days_since_enrolled_at",
        "stddev_score",
    )
    .orderBy(
        F.col("stddev_score").desc(),
        F.col("total_days_since_enrolled_at").desc(),
    )
)
