from pyspark.sql import functions as F

filtered_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") <= 7.6) & (F.col("valid_from") >= F.lit("2025-01-23 19:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        (F.col("valid_from") + F.expr("INTERVAL 1 DAYS")).alias("valid_from_plus_1d"),
        F.when(F.col("tariff").isin("FLAT", "EV"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
    )
)

annotated = (
    filtered_tariffs
    .select(
        "*",
        F.substring(F.trim(F.col("tariff_group")), 1, 4).alias("tariff_group_prefix"),
    )
)

filtered = (
    annotated
    .crossJoin(annotated.agg(F.min("rate").alias("overall_min_rate")))
    .filter(F.col("rate") > F.col("overall_min_rate"))
    .drop("overall_min_rate")
)

result = (
    filtered
    .groupingSets(
        [["tariff_group", "tariff"], ["tariff_group"]],
        "tariff_group", "tariff",
    )
    .agg(F.min("rate").alias("min_rate"))
)
