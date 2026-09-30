from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_len = Window.partitionBy("tariff_len")

cleaned = (
    df_tariff_windows
    .filter((F.col("valid_from") >= F.lit("2025-02-15 04:00:00").cast("timestamp")) | (F.col("tariff").isNotNull()))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "valid_to",
        "rate",
        "tariff_len",
        F.round(F.col("rate") - F.avg("rate").over(w_tariff_len), 2).alias("rate_vs_tariff_len_avg"),
        F.max("tariff_len").over(w_tariff_len).alias("tariff_len_max_tariff_len"),
    )
)

scored = windowed.withColumn("valid_to_label", F.date_format(F.col("valid_to"), "MMM yyyy"))

result = (
    scored
    .select(
        "id",
        "rate",
        "tariff_len",
        "rate_vs_tariff_len_avg",
        "tariff_len_max_tariff_len",
        "valid_to_label",
        F.abs(F.col("rate_vs_tariff_len_avg") - -1.6).alias("rate_vs_tariff_len_avg_distance"),
    )
)
