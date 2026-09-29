from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_commissioned_on_month_instal = Window.partitionBy(F.date_trunc("month", "commissioned_on"), "installed_kw_tier")
w_zone_len_tariff_by_installed = Window.partitionBy("zone_len", "tariff").orderBy("installed_kw", "id")
w_all_by_commissioned_on = Window.orderBy(F.col("commissioned_on").desc(), "id")

cleaned = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("M%")) & (F.col("installed_kw") > 141.6))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
        F.when(F.col("installed_kw") >= 284.0, "large").when(F.col("installed_kw") >= 141.6, "medium").otherwise("small").alias("installed_kw_tier"),
    )
)

zone_stats = (
    cleaned
    .groupBy("zone")
    .agg(F.percentile_approx("zone_len", 0.5).alias("median_zone_len"))
)

outliers = (
    cleaned.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.zone_len") >= F.col("g.median_zone_len"))
    .select(
        "s.*",
        "g.median_zone_len",
        F.round(F.col("s.zone_len") - F.col("g.median_zone_len"), 2).alias("zone_len_minus_median"),
    )
)

with_windows = (
    outliers
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "zone_len",
        "installed_kw_tier",
        "median_zone_len",
        "zone_len_minus_median",
        F.count(F.lit(1)).over(w_commissioned_on_month_instal).alias("rows_in_commissioned_on_month_installed_kw_tier"),
        F.ntile(10).over(w_zone_len_tariff_by_installed).alias("installed_kw_decile"),
        F.lag("zone_len").over(w_zone_len_tariff_by_installed).alias("prev_zone_len"),
        F.lag("zone_len_minus_median", 3).over(w_all_by_commissioned_on).alias("prev3_zone_len_minus_median"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("kwh").alias("min_reading_kwh"),
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.min_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
    )
)

profiled = (
    with_stats.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.min_reading_voltage", "m.avg_reading_kwh")
)

result = (
    profiled
    .select(
        "id",
        F.col("reading_quality_count").alias("m_reading_quality_count"),
        F.col("reading_count").alias("m_reading_count"),
    )
    .unpivot(
        ids=["id"],
        values=["m_reading_quality_count", "m_reading_count"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
