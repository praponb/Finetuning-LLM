from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_plus_1d_month_tar = Window.partitionBy(F.date_trunc("month", "valid_from_plus_1d"), "tariff")
w_tariff_by_valid_from = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), "id")
w_tariff_by_valid_from_ties = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc())
w_latest = Window.partitionBy("tariff_or_unknown", "tariff_or_unknown_2").orderBy(F.col("valid_from").desc(), F.col("id").desc())

cleaned = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        (F.col("valid_from") + F.expr("INTERVAL 1 DAYS")).alias("valid_from_plus_1d"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown_2"),
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        "valid_from_plus_1d",
        "tariff_or_unknown_2",
        F.sum("rate").over(w_valid_from_plus_1d_month_tar).alias("valid_from_plus_1d_month_tariff_sum_rate"),
        F.round(F.avg("rate").over(w_valid_from_plus_1d_month_tar), 2).alias("valid_from_plus_1d_month_tariff_avg_rate"),
        F.rank().over(w_tariff_by_valid_from).alias("valid_from_rank"),
        F.dense_rank().over(w_tariff_by_valid_from_ties).alias("valid_from_dense_rank"),
        F.round(F.col("rate") - F.avg("rate").over(w_valid_from_plus_1d_month_tar), 2).alias("rate_vs_valid_from_plus_1d_month_tariff_avg"),
    )
)

scored = (
    windowed
    .filter(F.col("valid_from_rank") > 5)
    .withColumn("tariff_or_unknown", F.coalesce(F.col("tariff"), F.lit("UNKNOWN")))
)

tariff_stats = (
    scored
    .groupBy("tariff")
    .agg(F.max("valid_from_plus_1d_month_tariff_avg_rate").alias("max_valid_from_plus_1d_month_tariff_avg_rate"))
)

vs_group = (
    scored.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.valid_from_plus_1d_month_tariff_avg_rate") == F.col("g.max_valid_from_plus_1d_month_tariff_avg_rate"))
    .select(
        "s.*",
        "g.max_valid_from_plus_1d_month_tariff_avg_rate",
        F.round(F.col("s.valid_from_plus_1d_month_tariff_avg_rate") - F.col("g.max_valid_from_plus_1d_month_tariff_avg_rate"), 2).alias("valid_from_plus_1d_month_tariff_avg_rate_minus_m"),
    )
)

latest_per_key = (
    vs_group
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .groupingSets(
        [["tariff_or_unknown_2", "valid_from_dense_rank", "tariff_or_unknown"], ["tariff_or_unknown_2"], ["valid_from_dense_rank"], []],
        "tariff_or_unknown_2", "valid_from_dense_rank", "tariff_or_unknown",
    )
    .agg(F.min("valid_to").alias("first_valid_to"))
    .orderBy(
        F.col("tariff_or_unknown_2").asc_nulls_last(),
        F.col("valid_from_dense_rank").asc_nulls_last(),
        F.col("tariff_or_unknown").asc_nulls_last(),
    )
)
