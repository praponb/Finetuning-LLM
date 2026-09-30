from pyspark.sql import functions as F

recent_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_from") < F.lit("2025-02-18 04:00:00").cast("timestamp")) & (F.col("tariff").isin("TOU", "FLAT")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.abs(F.col("rate") - 4.2).alias("rate_distance"),
        F.to_date(F.col("valid_from")).alias("valid_from_date"),
    )
)

tariff_stats = (
    recent_tariffs
    .groupBy("tariff")
    .agg(F.avg("tariff_len").alias("avg_tariff_len"))
)

vs_group = (
    recent_tariffs.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.tariff_len") > F.col("g.avg_tariff_len"))
    .select(
        "s.*",
        "g.avg_tariff_len",
        F.round(F.col("s.tariff_len") - F.col("g.avg_tariff_len"), 2).alias("tariff_len_minus_avg"),
    )
)

with_activity = (
    vs_group
    .crossJoin(vs_group.agg(F.avg("avg_tariff_len").alias("overall_avg_avg_tariff_len")))
    .filter(F.col("avg_tariff_len") > F.col("overall_avg_avg_tariff_len"))
    .drop("overall_avg_avg_tariff_len")
)

result = (
    with_activity
    .select(
        "id",
        "rate",
        "tariff_len",
        "avg_tariff_len",
    )
    .orderBy(F.col("tariff_len").asc(), F.col("avg_tariff_len").desc_nulls_first())
)
