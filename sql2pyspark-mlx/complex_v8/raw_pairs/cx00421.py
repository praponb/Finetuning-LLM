from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_rate_ties = Window.partitionBy("tariff").orderBy("rate")
w_tariff_by_rate_moving_6 = Window.partitionBy("tariff").orderBy("rate", "id").rowsBetween(-6, Window.currentRow)
w_tariff_by_rate = Window.partitionBy("tariff").orderBy("rate", "id")
w_tariff = Window.partitionBy("tariff")

cleaned = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_to")).between(6, 10)) & (F.hour(F.col("valid_from")).between(10, 14)))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        F.date_trunc("day", F.col("valid_to")).alias("valid_to_day"),
    )
)

tariff_stats = (
    cleaned
    .groupBy("tariff")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

above_average = (
    cleaned.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.round(F.cume_dist().over(w_tariff_by_rate_ties), 3).alias("rate_cume_dist"),
        F.round(F.avg("rate_minus_median").over(w_tariff_by_rate_moving_6), 2).alias("rate_minus_median_avg_7"),
        F.round(F.percent_rank().over(w_tariff_by_rate_ties), 3).alias("rate_percent_rank"),
        F.lag("valid_to").over(w_tariff_by_rate).alias("prev_valid_to"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("valid_to_day")).alias("days_since_valid_to_day"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

windowed = (
    annotated
    .withColumn("rate_minus_median_share_of_tariff", F.round(F.col("rate_minus_median") / F.nullif(F.sum("rate_minus_median").over(w_tariff), F.lit(0)), 4))
    .withColumn("rate_share_of_tariff", F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_tariff), F.lit(0)), 4))
)

result = (
    windowed
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
        "median_rate",
        "rate_minus_median",
        "rate_cume_dist",
        "rate_percent_rank",
        "prev_valid_to",
        "days_since_valid_to_day",
        "tariff_len",
        "tariff_or_unknown",
        "rate_share_of_tariff",
        F.round((F.unix_timestamp(F.col("prev_valid_to")) - F.unix_timestamp(F.col("valid_to"))) / 3600, 1).alias("hours_valid_to_to_prev_valid_to"),
    )
    .orderBy("rate")
)
