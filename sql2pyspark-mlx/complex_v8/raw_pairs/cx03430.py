from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_to_ties = Window.partitionBy("tariff").orderBy(F.col("valid_to").desc())
w_valid_to_month_tariff = Window.partitionBy(F.date_trunc("month", "valid_to"), "tariff")
w_tariff_by_rate = Window.partitionBy("tariff").orderBy("rate", "id")

valid_tariffs = (
    df_tariff_windows
    .filter((F.col("rate").between(3.7, 6.1)) & (F.col("valid_to") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

without_matches = (
    valid_tariffs
    .crossJoin(valid_tariffs.agg(F.max("rate").alias("overall_max_rate")))
    .filter(F.col("rate") < F.col("overall_max_rate"))
    .drop("overall_max_rate")
)

sequenced = (
    without_matches
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.round(F.percent_rank().over(w_tariff_by_valid_to_ties), 3).alias("valid_to_percent_rank"),
        F.round(F.col("rate") - F.avg("rate").over(w_valid_to_month_tariff), 2).alias("rate_vs_valid_to_month_tariff_avg"),
        F.min("rate").over(w_valid_to_month_tariff).alias("valid_to_month_tariff_min_rate"),
        F.round(F.cume_dist().over(w_tariff_by_rate), 3).alias("rate_cume_dist"),
    )
)

result = (
    sequenced
    .groupBy("valid_to")
    .pivot("tariff", ["FLAT", "TOU"])
    .agg(F.min("rate_vs_valid_to_month_tariff_avg"))
    .orderBy("valid_to")
)
