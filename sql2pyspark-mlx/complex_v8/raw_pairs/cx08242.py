from pyspark.sql import functions as F

filtered_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff").isin("EV", "FLAT")) & (F.col("valid_from") < F.lit("2025-02-16 11:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") * 1.1, 2).alias("rate_adj"),
        F.to_date(F.col("valid_to")).alias("valid_to_date"),
        F.date_format(F.col("valid_from"), "MMM yyyy").alias("valid_from_label"),
    )
)

screened = (
    filtered_tariffs
    .crossJoin(filtered_tariffs.agg(F.avg("rate_adj").alias("overall_avg_rate_adj")))
    .filter(F.col("rate_adj") > F.col("overall_avg_rate_adj"))
    .drop("overall_avg_rate_adj")
)

result = (
    screened
    .groupBy("valid_from", "valid_to")
    .pivot("tariff", ["EV", "TOU"])
    .agg(F.min("rate"))
)
