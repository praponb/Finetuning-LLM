from pyspark.sql import functions as F

matched = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

rollup_base = (
    matched
    .filter(F.hour(F.col("played_at")).between(8, 11))
    .groupBy(F.date_format("played_at", "yyyy-MM").alias("played_at_ym"))
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.sum("prior_rows_7d").alias("total_prior_rows_7d"),
    )
)

result = (
    rollup_base
    .select(
        "played_at_ym",
        "first_played_at",
        "total_prior_rows_7d",
        F.year(F.col("first_played_at")).alias("first_played_at_year"),
        F.lower(F.trim(F.col("played_at_ym"))).alias("played_at_ym_clean"),
    )
    .orderBy(
        "first_played_at",
        F.col("total_prior_rows_7d").asc_nulls_last(),
        "played_at_ym",
    )
    .limit(10)
)
