from pyspark.sql import functions as F

filtered = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.round(F.col("rate") * 0.25, 2).alias("rate_adj"),
        (F.col("valid_from") + F.expr("INTERVAL 15 MINUTES")).alias("valid_from_plus_15m"),
        F.when(F.col("rate") >= 5.7, "HIGH").when(F.col("rate") >= 3.0, "MEDIUM").otherwise("LOW").alias("rate_tier"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.coalesce(F.col("rate"), F.lit(0.0)).alias("rate_filled"),
        F.when(F.col("rate") >= 7.6, "HIGH").when(F.col("rate") >= 3.0, "MEDIUM").otherwise("LOW").alias("rate_tier_2"),
        F.when(F.col("rate_tier").isin("LOW", "MEDIUM"), "group_a").when(F.col("rate_tier") == "HIGH", "group_b").otherwise("other").alias("rate_tier_group"),
    )
)

tariff_stats = (
    scored
    .groupBy("tariff")
    .agg(F.avg("rate_adj").alias("avg_rate_adj"))
)

compared = (
    scored.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate_adj") > F.col("g.avg_rate_adj"))
    .select(
        "s.*",
        "g.avg_rate_adj",
        F.round(F.col("s.rate_adj") - F.col("g.avg_rate_adj"), 2).alias("rate_adj_minus_avg"),
    )
)

result = (
    compared
    .groupBy("rate_tier_2", "tariff")
    .pivot("rate_tier_group", ["other", "group_b", "group_a"])
    .agg(F.sum("rate_adj").alias("total"), F.count("rate_adj").alias("n"))
    .orderBy("rate_tier_2", "tariff")
)
