from pyspark.sql import functions as F

scoped = (
    df_tariff_windows
    .filter((F.col("tariff") != "FLAT") & (F.col("rate") < 7.6))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.abs(F.col("rate") - 4.2).alias("rate_distance"),
        F.when(F.col("rate") >= 5.7, "A").when(F.col("rate") >= 3.7, "B").otherwise("C").alias("rate_tier"),
    )
)

tariff_stats = (
    scoped
    .groupBy("tariff")
    .agg(F.avg("rate").alias("avg_rate"))
)

compared = (
    scoped.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

result = (
    compared
    .groupingSets(
        [["rate_tier", "tariff"], ["rate_tier"], []],
        "rate_tier", "tariff",
    )
    .agg(
        F.grouping("rate_tier").alias("is_total_rate_tier"),
        F.percentile_approx("avg_rate", 0.5).alias("median_avg_rate"),
        F.count("*").alias("row_count"),
    )
    .select(
        F.coalesce(F.col("rate_tier"), F.lit("ALL")).alias("rate_tier"),
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "is_total_rate_tier",
        "median_avg_rate",
        "row_count",
    )
    .orderBy(F.col("rate_tier").asc_nulls_last(), F.col("tariff").asc_nulls_last())
)
