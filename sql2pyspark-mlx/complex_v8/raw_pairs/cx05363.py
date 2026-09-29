from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_installed_kw_adj_mov = Window.partitionBy("zone").orderBy(F.col("installed_kw_adj").desc(), "id").rowsBetween(-1, Window.currentRow)
w_zone_by_installed_kw_adj_tie = Window.partitionBy("zone").orderBy(F.col("installed_kw_adj").desc())

valid = (
    df_sites
    .filter(F.col("tariff") == "EV")
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.round(F.col("installed_kw") * 0.9, 2).alias("installed_kw_adj"),
        F.add_months(F.col("commissioned_on"), 12).alias("commissioned_on_plus_12m"),
        F.substring(F.trim(F.col("site_name")), 1, 4).alias("site_name_prefix"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("installed_kw").over(w_zone_by_installed_kw_adj_mov).alias("installed_kw_sum_2"),
        F.round(F.percent_rank().over(w_zone_by_installed_kw_adj_tie), 3).alias("installed_kw_adj_percent_rank"),
    )
)

detail_rows_readings = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") > 48.2), "inner")
    .select(
        "s.id",
        "s.zone",
        "s.installed_kw",
        "s.commissioned_on_plus_12m",
        "s.site_name_prefix",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

tagged = (
    detail_rows_readings
    .select(
        "*",
        F.when(F.col("zone").isin("Z4", "Z2"), "group_a").when(F.col("zone") == "Z3", "group_b").otherwise("other").alias("zone_group"),
    )
)

with_parent_reading = (
    tagged.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .select(
        "s.id",
        "s.commissioned_on_plus_12m",
        "s.reading_id",
        "s.reading_quality",
        "s.zone_group",
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

zone_group_stats = (
    with_parent_reading
    .groupBy("zone_group")
    .agg(F.avg("reading_kwh_2").alias("avg_reading_kwh_2"))
)

above_average = (
    with_parent_reading.alias("s")
    .join(zone_group_stats.alias("g"), F.col("g.zone_group") == F.col("s.zone_group"), "inner")
    .filter(F.col("s.reading_kwh_2") > F.col("g.avg_reading_kwh_2"))
    .select(
        "s.*",
        "g.avg_reading_kwh_2",
        F.round(F.col("s.reading_kwh_2") - F.col("g.avg_reading_kwh_2"), 2).alias("reading_kwh_2_minus_avg"),
    )
)

result = (
    above_average
    .filter(F.col("zone_group").isin("group_b", "group_a"))
    .select(
        "id",
        "reading_quality",
        "zone_group",
        "reading_read_at",
        "reading_kwh_2",
        "avg_reading_kwh_2",
        F.substring(F.trim(F.col("reading_quality")), 1, 3).alias("reading_quality_prefix"),
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("id").cast("string")).alias("reading_quality_ref"),
    )
)
