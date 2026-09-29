from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_valid_to_m = Window.orderBy("days_since_valid_to", "id").rowsBetween(-1, Window.currentRow)
w_all_by_days_since_valid_to = Window.orderBy("days_since_valid_to", "id")
w_valid_to_month_tariff_len = Window.partitionBy(F.month("valid_to"), "tariff_len")

valid_tariffs = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(6, 9))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.when(F.col("tariff").isin("EV", "FLAT"), "group_a").when(F.col("tariff") == "TOU", "group_b").otherwise("other").alias("tariff_group"),
        F.substring(F.trim(F.col("tariff")), 1, 2).alias("tariff_prefix"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

tariff_group_stats = (
    valid_tariffs
    .groupBy("tariff_group")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

above_average = (
    valid_tariffs.alias("s")
    .join(tariff_group_stats.alias("g"), F.col("g.tariff_group") == F.col("s.tariff_group"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

derived = (
    above_average
    .select(
        "*",
        F.length(F.trim(F.col("tariff_prefix"))).alias("tariff_prefix_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("valid_to")).alias("days_since_valid_to"),
    )
)

tariff_group_stats_2 = (
    derived
    .groupBy("tariff_group")
    .agg(F.avg("rate_minus_median").alias("avg_rate_minus_median"))
)

vs_group = (
    derived.alias("s")
    .join(tariff_group_stats_2.alias("g"), F.col("g.tariff_group") == F.col("s.tariff_group"), "inner")
    .filter(F.col("s.rate_minus_median") > F.col("g.avg_rate_minus_median"))
    .select(
        "s.*",
        "g.avg_rate_minus_median",
        F.round(F.col("s.rate_minus_median") - F.col("g.avg_rate_minus_median"), 2).alias("rate_minus_median_minus_avg"),
    )
)

tariff_group_stats_3 = (
    vs_group
    .groupBy("tariff_group")
    .agg(F.percentile_approx("tariff_prefix_len", 0.5).alias("median_tariff_prefix_len"))
)

above_average_2 = (
    vs_group.alias("s")
    .join(tariff_group_stats_3.alias("g"), F.col("g.tariff_group") == F.col("s.tariff_group"), "inner")
    .filter(F.col("s.tariff_prefix_len") >= F.col("g.median_tariff_prefix_len"))
    .select(
        "s.*",
        "g.median_tariff_prefix_len",
        F.round(F.col("s.tariff_prefix_len") - F.col("g.median_tariff_prefix_len"), 2).alias("tariff_prefix_len_minus_median"),
    )
)

with_activity = (
    above_average_2
    .crossJoin(above_average_2.agg(F.min("tariff_len").alias("overall_min_tariff_len")))
    .filter(F.col("tariff_len") > F.col("overall_min_tariff_len"))
    .drop("overall_min_tariff_len")
)

sequenced = (
    with_activity
    .select(
        "*",
        F.round(F.avg("rate_minus_median_minus_avg").over(w_all_by_days_since_valid_to_m), 2).alias("rate_minus_median_minus_avg_avg_2"),
        F.round(F.cume_dist().over(w_all_by_days_since_valid_to), 3).alias("days_since_valid_to_cume_dist"),
        F.size(F.collect_set("tariff_group").over(w_valid_to_month_tariff_len)).alias("distinct_tariff_group_in_valid_to_month_tariff_l"),
    )
)

result = (
    sequenced
    .groupBy("valid_to")
    .pivot("tariff_group", ["group_b", "other"])
    .agg(F.min("tariff_len"))
)
