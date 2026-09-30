from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_from_month_tariff_by_r = Window.partitionBy(F.month("valid_from"), "tariff").orderBy("rate")
w_valid_from_month_tariff_by_r_2 = Window.partitionBy(F.month("valid_from"), "tariff").orderBy("rate", "id")

recent_tariffs = (
    df_tariff_windows
    .filter(~F.col("tariff").isin("EV", "FLAT"))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug_2"),
    )
)

screened = (
    recent_tariffs
    .crossJoin(recent_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

metrics = (
    screened
    .select(
        "*",
        F.round(F.percent_rank().over(w_valid_from_month_tariff_by_r), 3).alias("rate_percent_rank"),
        F.dense_rank().over(w_valid_from_month_tariff_by_r_2).alias("rate_dense_rank"),
    )
)

tariff_stats = (
    metrics
    .groupBy("tariff")
    .agg(F.avg("rate_percent_rank").alias("avg_rate_percent_rank"))
)

above_average = (
    metrics.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate_percent_rank") > F.col("g.avg_rate_percent_rank"))
    .select(
        "s.*",
        "g.avg_rate_percent_rank",
        F.round(F.col("s.rate_percent_rank") - F.col("g.avg_rate_percent_rank"), 2).alias("rate_percent_rank_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["rate_dense_rank", "tariff_len"], ["rate_dense_rank"], ["tariff_len"], []],
        "rate_dense_rank", "tariff_len",
    )
    .agg(
        F.percentile_approx("rate_percent_rank_minus_avg", 0.5).alias("median_rate_percent_rank_minus_avg"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
    )
    .orderBy(
        F.col("rate_dense_rank").asc_nulls_last(),
        F.col("tariff_len").asc_nulls_last(),
    )
)
