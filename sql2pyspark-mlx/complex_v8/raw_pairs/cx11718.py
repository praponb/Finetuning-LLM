from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 3)) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.abs(F.col("score") - 50.9).alias("score_distance"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

rollup_base = (
    cleaned_enrollments
    .groupBy(F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"))
    .agg(
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.round(100 * F.count(F.when(F.col("term").isin("2025T2", "2025T1", "2024T1"), 1)) / F.count("*"), 1).alias("pct_term_in_2025t2_2025t1_2024t1"),
        F.round(F.sum("score_distance"), 2).alias("total_score_distance"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
        F.count("*").alias("cnt"),
        F.round(100 * F.count(F.when(F.col("term").isin("2025T2", "2025T1"), 1)) / F.count("*"), 1).alias("pct_term_in_2025t2_2025t1"),
    )
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("pct_term_in_2025t2_2025t1_2024t1").alias("overall_min_pct_term_in_2025t2_2025t1_2024t1")))
    .filter(F.col("pct_term_in_2025t2_2025t1_2024t1") > F.col("overall_min_pct_term_in_2025t2_2025t1_2024t1"))
    .drop("overall_min_pct_term_in_2025t2_2025t1_2024t1")
)

result = (
    without_matches
    .select(
        "enrolled_at_ym",
        "total_score_distance",
        "pct_term_in_2025t2_2025t1",
        "pct_term_in_2025t2_2025t1_2024t1",
        "stddev_score",
    )
    .unpivot(
        ids=["enrolled_at_ym"],
        values=["total_score_distance", "pct_term_in_2025t2_2025t1", "pct_term_in_2025t2_2025t1_2024t1", "stddev_score"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("enrolled_at_ym", "metric_name")
)
