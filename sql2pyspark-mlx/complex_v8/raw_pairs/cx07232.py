from pyspark.sql import functions as F

prepared = (
    df_tariff_windows
    .filter((F.col("tariff").isin("EV", "TOU")) & (F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp")))
)

annotated = (
    prepared
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.round(F.col("rate") * 1.5, 2).alias("rate_adj"),
    )
)

rollup_base = (
    annotated
    .groupBy(
        F.year("valid_from").alias("valid_from_year"),
        "tariff",
        F.date_trunc("month", "valid_to").alias("valid_to_month"),
    )
    .agg(F.min("rate_adj").alias("min_rate_adj"), F.count("*").alias("n_rows"))
    .filter(F.col("n_rows") >= 2)
)

scored = (
    rollup_base
    .withColumn("valid_to_month_plus_30m", F.col("valid_to_month") + F.expr("INTERVAL 30 MINUTES"))
)

enriched = scored.withColumn("valid_to_month_date", F.to_date(F.col("valid_to_month")))

result = (
    enriched
    .select(
        "valid_from_year",
        "tariff",
        "valid_to_month",
        "min_rate_adj",
        "n_rows",
        "valid_to_month_plus_30m",
        "valid_to_month_date",
        F.round(F.col("valid_from_year") / F.nullif(F.col("min_rate_adj"), F.lit(0)), 3).alias("valid_from_year_per_min_rate_adj"),
    )
    .orderBy(
        F.col("valid_from_year").desc(),
        F.col("valid_to_month_plus_30m").desc_nulls_first(),
        "tariff",
        "valid_to_month",
    )
    .limit(10)
)
