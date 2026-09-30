from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter((F.to_date(F.col("valid_to")) < F.lit("2025-03-12").cast("date")) | (F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("rate") >= 5.9, "large").when(F.col("rate") >= 3.7, "medium").otherwise("small").alias("rate_tier"),
        F.lower(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

without_matches = (
    prepared_tariffs
    .crossJoin(prepared_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

result = (
    without_matches
    .groupBy("valid_to")
    .pivot("tariff", ["TOU", "FLAT", "EV"])
    .agg(F.sum("rate"))
    .orderBy("valid_to")
)
