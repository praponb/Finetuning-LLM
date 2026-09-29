from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff_or_unknown").orderBy(F.col("valid_to").desc(), F.col("id").desc())

base_tariffs = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-12 23:00:00").cast("timestamp")) | (F.col("rate") >= 3.7))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

with_activity = (
    base_tariffs
    .crossJoin(base_tariffs.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

qualified = (
    with_activity
    .crossJoin(with_activity.agg(F.min("rate").alias("overall_min_rate")))
    .filter(F.col("rate") > F.col("overall_min_rate"))
    .drop("overall_min_rate")
)

latest_per_key = (
    qualified
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tariff_stats = (
    latest_per_key
    .groupBy("tariff")
    .agg(F.avg("rate").alias("avg_rate"))
)

compared = (
    latest_per_key.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

derived = (
    compared
    .filter(F.col("tariff_ref").isNotNull())
    .select(
        "*",
        F.abs(F.col("avg_rate") - 5.9).alias("avg_rate_distance"),
        F.initcap(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

totals = (
    derived
    .groupBy(
        "tariff",
        F.date_trunc("month", "valid_from").alias("valid_from_month"),
        "tariff_or_unknown",
    )
    .agg(
        F.max("valid_to").alias("last_valid_to"),
        F.round(F.avg("rate_minus_avg"), 2).alias("avg_rate_minus_avg"),
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("tariff") != "FLAT", F.col("rate_minus_avg"))).alias("rate_minus_avg_tariff_flat"),
    )
)

result = (
    totals
    .filter(F.month(F.col("valid_from_month")).isin(2, 3))
    .select(
        "tariff",
        "valid_from_month",
        "distinct_ids",
        "rate_minus_avg_tariff_flat",
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
        F.dayofweek(F.col("last_valid_to")).alias("last_valid_to_dayofweek"),
    )
    .orderBy(
        F.col("last_valid_to_dayofweek").desc_nulls_first(),
        F.col("rate_minus_avg_tariff_flat").desc(),
    )
)
