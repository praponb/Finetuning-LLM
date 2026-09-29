from pyspark.sql import functions as F

scoped = (
    df_tariff_windows
    .filter((F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")) & (F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.when(F.col("rate") >= 5.7, "HIGH").when(F.col("rate") >= 3.0, "MEDIUM").otherwise("LOW").alias("rate_tier"),
        F.when(F.col("rate") >= 5.5, "HIGH").when(F.col("rate") >= 4.2, "MEDIUM").otherwise("LOW").alias("rate_tier_2"),
    )
)

tagged = (
    scoped
    .select(
        "id",
        "tariff",
        "rate",
        "rate_tier",
        F.concat(F.col("rate_tier"), F.lit("#"), F.col("id").cast("string")).alias("rate_tier_ref"),
        F.month(F.col("valid_from")).alias("valid_from_month"),
    )
)

rollup_base = (
    tagged
    .groupBy(
        "valid_from_month",
        F.when(F.col("rate") >= 5.9, "HIGH").when(F.col("rate") >= 3.0, "MEDIUM").otherwise("LOW").alias("rate_tier_2"),
        "rate_tier",
    )
    .agg(
        F.sum("valid_from_month").alias("total_valid_from_month"),
        F.sum(F.when(F.col("tariff").isNotNull(), F.col("rate"))).alias("rate_tariff_is_not_null"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.percentile_approx("valid_from_month", 0.5).alias("median_valid_from_month"),
    )
)

result = (
    rollup_base
    .groupBy("median_valid_from_month")
    .pivot("rate_tier_2", ["LOW", "MEDIUM"])
    .agg(F.min("total_valid_from_month").alias("min_v"), F.sum("total_valid_from_month").alias("sum_v"))
    .orderBy("median_valid_from_month")
)
