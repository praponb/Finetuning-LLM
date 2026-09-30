from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 10)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 62.5, "A").when(F.col("score") >= 34.2, "B").otherwise("C").alias("score_tier"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.substring(F.trim(F.col("grade_status")), 1, 3).alias("grade_status_prefix"),
    )
)

totals = (
    base_enrollments
    .groupBy(
        F.to_date("enrolled_at").alias("enrolled_at_day"),
        "grade_status",
        "score_tier",
    )
    .agg(
        F.min("score").alias("min_score"),
        F.sum(F.when(F.col("term").isin("2025T1", "2024T1", "2025T2"), 1).otherwise(0)).alias("n_term_in_2025t1_2024t1_2025t2"),
    )
)

annotated = (
    totals
    .filter(F.col("min_score") <= 55.0)
    .select(
        "enrolled_at_day",
        "grade_status",
        "score_tier",
        "n_term_in_2025t1_2024t1_2025t2",
        F.date_add(F.col("enrolled_at_day"), 30).alias("enrolled_at_day_plus_30d"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at_day")).alias("days_since_enrolled_at_day"),
    )
)

without_matches = (
    annotated
    .crossJoin(annotated.agg(F.avg("days_since_enrolled_at_day").alias("overall_avg_days_since_enrolled_at_day")))
    .filter(F.col("days_since_enrolled_at_day") > F.col("overall_avg_days_since_enrolled_at_day"))
    .drop("overall_avg_days_since_enrolled_at_day")
)

result = (
    without_matches
    .select(
        "enrolled_at_day",
        "grade_status",
        F.col("n_term_in_2025t1_2024t1_2025t2").alias("m_n_term_in_2025t1_2024t1_2025t2"),
        F.col("days_since_enrolled_at_day").alias("m_days_since_enrolled_at_day"),
    )
    .unpivot(
        ids=["enrolled_at_day", "grade_status"],
        values=["m_n_term_in_2025t1_2024t1_2025t2", "m_days_since_enrolled_at_day"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("enrolled_at_day", "grade_status", "metric")
)
