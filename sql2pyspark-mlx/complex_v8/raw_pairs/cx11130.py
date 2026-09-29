from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_rate = Window.orderBy("rate", "id")
w_all_by_rate_ties = Window.orderBy("rate")

scoped_tariffs = (
    df_tariff_windows
    .filter((F.to_date(F.col("valid_to")) < F.lit("2025-04-05").cast("date")) | (F.col("rate") > 3.0))
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.when(F.col("tariff").isin("FLAT", "TOU"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other").alias("tariff_group"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

windowed = (
    scoped_tariffs
    .select(
        "id",
        "tariff",
        "valid_from",
        "rate",
        "tariff_ref",
        "tariff_slug",
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_all_by_rate))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.lead("valid_from", 2).over(w_all_by_rate).alias("next2_valid_from"),
        F.dense_rank().over(w_all_by_rate_ties).alias("rate_dense_rank"),
    )
)

tariff_stats = (
    windowed
    .groupBy("tariff")
    .agg(F.avg("rate").alias("avg_rate"))
)

compared = (
    windowed.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

tariff_stats_2 = (
    compared
    .groupBy("tariff")
    .agg(F.percentile_approx("rate", 0.5).alias("median_rate"))
)

compared_2 = (
    compared.alias("s")
    .join(tariff_stats_2.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.rate") >= F.col("g.median_rate"))
    .select(
        "s.*",
        "g.median_rate",
        F.round(F.col("s.rate") - F.col("g.median_rate"), 2).alias("rate_minus_median"),
    )
)

result = (
    compared_2
    .select(
        "id",
        F.col("minutes_since_prev_valid_from").alias("minutes_since_prev_valid_from_v"),
        F.col("rate_minus_avg").alias("m_rate_minus_avg"),
        F.col("median_rate").alias("median_rate_v"),
        F.col("rate_minus_median").alias("rate_minus_median_v"),
    )
    .unpivot(
        ids=["id"],
        values=["minutes_since_prev_valid_from_v", "m_rate_minus_avg", "median_rate_v", "rate_minus_median_v"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
