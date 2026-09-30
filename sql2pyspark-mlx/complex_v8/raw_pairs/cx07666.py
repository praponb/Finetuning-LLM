from pyspark.sql import functions as F

covered = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

totals = (
    covered
    .groupBy(
        F.to_date("enrolled_at").alias("enrolled_at_day"),
        "student_id",
        "prior_rows_1d",
    )
    .agg(
        F.percentile_approx("prior_score_1d", 0.5).alias("median_prior_score_1d"),
        F.round(F.avg("prior_rows_1d"), 2).alias("avg_prior_rows_1d"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    totals
    .select(
        "enrolled_at_day",
        "student_id",
        "prior_rows_1d",
        "median_prior_score_1d",
        "avg_prior_rows_1d",
        "row_count",
        F.dayofweek(F.col("enrolled_at_day")).alias("enrolled_at_day_dayofweek"),
    )
    .orderBy(F.col("avg_prior_rows_1d").asc_nulls_last())
)
