from pyspark.sql import functions as F

base = (
    df_tariff_windows
    .filter((F.col("tariff") != "FLAT") & (F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") * 0.25, 2).alias("rate_adj"),
        F.round(F.col("rate") * 1.1, 2).alias("rate_adj_2"),
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

enriched = (
    base
    .filter(F.upper(F.trim(F.col("tariff_clean"))).like("F%"))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_clean",
        F.abs(F.col("rate_adj") - 9.0).alias("rate_adj_distance"),
        (F.col("rate") + F.col("rate_adj_2")).alias("rate_plus_rate_adj_2"),
    )
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("rate_plus_rate_adj_2") >= 11.9, "large").when(F.col("rate_plus_rate_adj_2") >= 7.4, "medium").otherwise("small").alias("rate_plus_rate_adj_2_tier"),
        F.date_format("valid_from", "yyyy-MM").alias("valid_from_ym"),
        F.when(F.col("rate") >= 6.1, "A").when(F.col("rate") >= 3.7, "B").otherwise("C").alias("rate_tier"),
    )
    .agg(
        F.max("rate_plus_rate_adj_2").alias("max_rate_plus_rate_adj_2"),
        F.round(F.avg("rate"), 2).alias("avg_rate"),
        F.round(F.stddev("rate_plus_rate_adj_2"), 2).alias("stddev_rate_plus_rate_adj_2"),
        F.max("valid_from").alias("last_valid_from"),
        F.max("rate").alias("max_rate_check"),
    )
    .filter(F.col("max_rate_check") > 4.2)
    .drop("max_rate_check")
)

qualified = (
    stats
    .crossJoin(stats.agg(F.max("avg_rate").alias("overall_max_avg_rate")))
    .filter(F.col("avg_rate") < F.col("overall_max_avg_rate"))
    .drop("overall_max_avg_rate")
)

without_matches = (
    qualified
    .crossJoin(qualified.agg(F.avg("stddev_rate_plus_rate_adj_2").alias("overall_avg_stddev_rate_plus_rate_adj_2")))
    .filter(F.col("stddev_rate_plus_rate_adj_2") > F.col("overall_avg_stddev_rate_plus_rate_adj_2"))
    .drop("overall_avg_stddev_rate_plus_rate_adj_2")
)

result = (
    without_matches
    .groupBy("last_valid_from", "rate_plus_rate_adj_2_tier")
    .pivot("rate_tier", ["A", "C"])
    .agg(F.count("stddev_rate_plus_rate_adj_2"))
)
