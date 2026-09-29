from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_zone_by_commissioned_on = Window.partitionBy("zone").orderBy(F.col("commissioned_on").asc_nulls_last(), "id")
w_zone_len_by_commissioned_on = Window.partitionBy("zone_len").orderBy(F.col("commissioned_on").desc(), "id")

base = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
    )
)

scored = (
    base
    .filter(F.col("commissioned_on").isNotNull())
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on"),
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.sum("zone_len").over(w_zone).alias("zone_sum_zone_len"),
        F.size(F.collect_set("tariff").over(w_zone)).alias("distinct_tariff_in_zone"),
        F.rank().over(w_zone_by_commissioned_on).alias("commissioned_on_rank"),
        F.datediff(F.col("commissioned_on"), F.lag("commissioned_on").over(w_zone_len_by_commissioned_on)).alias("days_since_prev_commissioned_on"),
        F.round(F.cume_dist().over(w_zone_len_by_commissioned_on), 3).alias("commissioned_on_cume_dist"),
        F.lead("days_since_commissioned_on").over(w_zone_len_by_commissioned_on).alias("next_days_since_commissioned_on"),
    )
)

tariff_stats = (
    with_trends
    .groupBy("tariff")
    .agg(F.percentile_approx("days_since_commissioned_on", 0.5).alias("median_days_since_commissioned_on"))
)

compared = (
    with_trends.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.days_since_commissioned_on") >= F.col("g.median_days_since_commissioned_on"))
    .select(
        "s.*",
        "g.median_days_since_commissioned_on",
        F.round(F.col("s.days_since_commissioned_on") - F.col("g.median_days_since_commissioned_on"), 2).alias("days_since_commissioned_on_minus_median"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("voltage").alias("max_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    compared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.max_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

derived = (
    summarized_rows
    .select(
        "*",
        F.round(F.col("commissioned_on_rank") / F.nullif(F.col("distinct_tariff_in_zone"), F.lit(0)), 3).alias("commissioned_on_rank_per_distinct_tariff_in_zone"),
        F.lower(F.trim(F.col("zone"))).alias("zone_clean"),
    )
)

labeled = (
    derived
    .select(
        "*",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        F.coalesce(F.col("max_reading_voltage"), F.lit(0.0)).alias("max_reading_voltage_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("commissioned_on")).alias("days_since_commissioned_on_2"),
    )
)

result = (
    labeled
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
        "zone_len",
        "zone_sum_zone_len",
        "distinct_tariff_in_zone",
        "commissioned_on_rank",
        "next_days_since_commissioned_on",
        "median_days_since_commissioned_on",
        "days_since_commissioned_on_minus_median",
        "last_reading_read_at",
        "max_reading_voltage",
        "commissioned_on_rank_per_distinct_tariff_in_zone",
        "zone_clean",
        "max_reading_voltage_filled",
        F.concat(F.col("zone_or_unknown"), F.lit("#"), F.col("reading_count").cast("string")).alias("zone_or_unknown_ref"),
    )
    .orderBy(F.col("tariff").asc_nulls_last(), "median_days_since_commissioned_on")
)
