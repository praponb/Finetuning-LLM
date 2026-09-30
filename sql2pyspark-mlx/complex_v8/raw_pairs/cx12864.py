from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_from")).between(7, 13)) & (F.col("tariff") == "FLAT"))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

annotated = (
    prepared_tariffs
    .filter(F.hour(F.col("valid_to")).between(8, 10))
    .select(
        "*",
        F.abs(F.col("rate") - 5.9).alias("rate_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

labeled = (
    annotated
    .select(
        "*",
        F.when(F.col("rate") >= 5.9, "A").when(F.col("rate") >= 3.0, "B").otherwise("C").alias("rate_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
        F.initcap(F.trim(F.col("tariff_slug"))).alias("tariff_slug_clean"),
    )
)

result = (
    labeled
    .rollup("rate_tier", "tariff")
    .agg(
        F.min("rate_distance").alias("min_rate_distance"),
        F.max("rate_distance").alias("max_rate_distance"),
        F.round(F.sum("days_since_valid_to") / F.nullif(F.sum("rate"), F.lit(0)), 3).alias("days_since_valid_to_to_rate_ratio"),
    )
    .orderBy(F.col("rate_tier").asc_nulls_last(), F.col("tariff").asc_nulls_last())
)
