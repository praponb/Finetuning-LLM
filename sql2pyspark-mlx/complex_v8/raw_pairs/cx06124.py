from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_by_valid_from_moving = Window.partitionBy("tariff").orderBy("valid_from", "id").rowsBetween(-3, Window.currentRow)
w_valid_from_month = Window.partitionBy(F.date_trunc("month", "valid_from"))
w_tariff_by_valid_from_running = Window.partitionBy("tariff").orderBy(F.col("valid_from").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_valid_from_running_2 = Window.partitionBy("tariff").orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_rate_avg_4 = Window.partitionBy("tariff").orderBy("rate_avg_4", "id")
w_tariff_by_rate_avg_4_running = Window.partitionBy("tariff").orderBy("rate_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_rate_avg_4_full = Window.partitionBy("tariff").orderBy("rate_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_tariff_windows
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
        F.substring(F.trim(F.col("tariff")), 1, 4).alias("tariff_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("valid_from")).alias("days_since_valid_from"),
    )
)

with_trends = (
    cleaned
    .select(
        "*",
        F.round(F.avg("rate").over(w_tariff_by_valid_from_moving), 2).alias("rate_avg_4"),
        F.min("days_since_valid_from").over(w_valid_from_month).alias("valid_from_month_min_days_since_valid_from"),
        F.first("valid_to", ignorenulls=True).over(w_tariff_by_valid_from_running).alias("first_known_valid_to"),
        F.first("rate", ignorenulls=True).over(w_tariff_by_valid_from_running_2).alias("first_known_rate"),
        F.max("rate").over(w_valid_from_month).alias("valid_from_month_max_rate"),
    )
)

filtered = (
    with_trends
    .crossJoin(with_trends.agg(F.min("valid_from_month_min_days_since_valid_from").alias("overall_min_valid_from_month_min_days_since_vali")))
    .filter(F.col("valid_from_month_min_days_since_valid_from") > F.col("overall_min_valid_from_month_min_days_since_vali"))
    .drop("overall_min_valid_from_month_min_days_since_vali")
)

windowed = (
    filtered
    .select(
        "*",
        (F.col("days_since_valid_from") - F.lag("days_since_valid_from").over(w_tariff_by_rate_avg_4)).alias("days_since_valid_from_change"),
        F.round((F.unix_timestamp("valid_from") - F.unix_timestamp(F.lag("valid_from").over(w_tariff_by_rate_avg_4))) / 60, 1).alias("minutes_since_prev_valid_from"),
        F.count(F.lit(1)).over(w_tariff_by_rate_avg_4_running).alias("running_count"),
        F.last("valid_to").over(w_tariff_by_rate_avg_4_full).alias("last_valid_to"),
        F.min("valid_from_month_max_rate").over(w_tariff_by_rate_avg_4_running).alias("min_valid_from_month_max_rate_so_far"),
    )
)

scored = (
    windowed
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_ref",
        "tariff_prefix",
        "days_since_valid_from",
        "rate_avg_4",
        "valid_from_month_min_days_since_valid_from",
        "first_known_valid_to",
        "first_known_rate",
        "days_since_valid_from_change",
        "minutes_since_prev_valid_from",
        "running_count",
        F.to_date(F.col("last_valid_to")).alias("last_valid_to_date"),
        F.length(F.trim(F.col("tariff_ref"))).alias("tariff_ref_len"),
        F.when(F.col("running_count") >= 14, "large").when(F.col("running_count") >= 6, "medium").otherwise("small").alias("running_count_tier"),
    )
)

running_count_tier_stats = (
    scored
    .groupBy("running_count_tier")
    .agg(F.avg("rate_avg_4").alias("avg_rate_avg_4"))
)

compared = (
    scored.alias("s")
    .join(running_count_tier_stats.alias("g"), F.col("g.running_count_tier") == F.col("s.running_count_tier"), "inner")
    .filter(F.col("s.rate_avg_4") > F.col("g.avg_rate_avg_4"))
    .select(
        "s.*",
        "g.avg_rate_avg_4",
        F.round(F.col("s.rate_avg_4") - F.col("g.avg_rate_avg_4"), 2).alias("rate_avg_4_minus_avg"),
    )
)

screened = (
    compared
    .crossJoin(compared.agg(F.avg("rate").alias("overall_avg_rate")))
    .filter(F.col("rate") > F.col("overall_avg_rate"))
    .drop("overall_avg_rate")
)

result = (
    screened
    .filter(F.col("rate_avg_4").between(3.8, 6.2))
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "tariff_ref",
        "days_since_valid_from",
        "rate_avg_4",
        "valid_from_month_min_days_since_valid_from",
        "first_known_valid_to",
        "first_known_rate",
        "days_since_valid_from_change",
        "running_count",
        "tariff_ref_len",
        F.concat(F.col("tariff_ref"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref_ref"),
    )
    .orderBy("tariff_ref_ref")
)
