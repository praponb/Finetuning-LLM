from pyspark.sql import functions as F

cleaned_tariffs = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_to")).between(9, 13)) | (F.col("tariff").isin("EV", "TOU")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

enriched = (
    cleaned_tariffs
    .filter(F.col("tariff_or_unknown") == "TOU")
    .select(
        "*",
        F.concat_ws("|", F.col("tariff_or_unknown"), F.col("tariff")).alias("tariff_or_unknown_tariff_label"),
    )
)

qualified = (
    enriched
    .crossJoin(enriched.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

tariff_stats = (
    qualified
    .groupBy("tariff")
    .agg(F.max("rate").alias("max_rate"))
)

vs_group = (
    qualified.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") == F.col("g.max_rate"))
    .select(
        "s.*",
        "g.max_rate",
        F.round(F.col("s.rate") - F.col("g.max_rate"), 2).alias("rate_minus_max"),
    )
)

per_group = (
    vs_group
    .filter(F.col("rate_minus_max") < 1.6)
    .groupBy(
        "tariff_or_unknown",
        F.date_trunc("month", "valid_from").alias("valid_from_month"),
    )
    .agg(
        F.min("rate_minus_max").alias("min_rate_minus_max"),
        F.count("*").alias("cnt"),
        F.percentile_approx("max_rate", 0.5).alias("median_max_rate"),
        F.round(F.stddev("rate_minus_max"), 2).alias("stddev_rate_minus_max"),
        F.max("rate_minus_max").alias("max_rate_minus_max"),
        F.max("rate").alias("max_rate_2"),
    )
)

result = (
    per_group
    .select(
        "tariff_or_unknown",
        "valid_from_month",
        F.col("max_rate_minus_max").alias("m_max_rate_minus_max"),
        F.col("max_rate_2").alias("m_max_rate_2"),
    )
    .unpivot(
        ids=["tariff_or_unknown", "valid_from_month"],
        values=["m_max_rate_minus_max", "m_max_rate_2"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
