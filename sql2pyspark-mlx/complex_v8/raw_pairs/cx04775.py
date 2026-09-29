from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reading_kwh = Window.orderBy(F.col("reading_kwh").desc(), "id", "reading_id")
w_reading_id = Window.partitionBy("reading_id")

base = (
    df_sites
    .filter((F.col("zone") == "Z3") & (F.col("tariff") == "TOU"))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("quarter", F.col("commissioned_on")).alias("commissioned_on_quarter"),
        F.upper(F.trim(F.col("tariff"))).alias("tariff_clean"),
    )
)

derived = (
    base
    .select(
        "*",
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
    )
)

zone_stats = (
    derived
    .groupBy("zone")
    .agg(F.avg("installed_kw").alias("avg_installed_kw"))
)

above_average = (
    derived.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.installed_kw") > F.col("g.avg_installed_kw"))
    .select(
        "s.*",
        "g.avg_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.avg_installed_kw"), 2).alias("installed_kw_minus_avg"),
    )
)

tagged = (
    above_average
    .select(
        "id",
        "site_name",
        "installed_kw",
        "commissioned_on",
        "tariff_clean",
        "commissioned_on_label",
        "installed_kw_minus_avg",
        F.dayofweek(F.col("commissioned_on_quarter")).alias("commissioned_on_quarter_dayofweek"),
        F.substring(F.trim(F.col("site_name")), 1, 3).alias("site_name_prefix"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

line_level = (
    tagged.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.installed_kw",
        "s.tariff_clean",
        "s.installed_kw_minus_avg",
        "s.site_name_prefix",
        "s.installed_kw_bucket",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

ranked = (
    line_level
    .select(
        "id",
        "installed_kw",
        "tariff_clean",
        "installed_kw_minus_avg",
        "site_name_prefix",
        "installed_kw_bucket",
        "reading_id",
        "reading_voltage",
        (F.col("installed_kw") - F.lag("installed_kw").over(w_all_by_reading_kwh)).alias("installed_kw_change"),
        F.lag("installed_kw_minus_avg", 1, 0).over(w_all_by_reading_kwh).alias("prev_installed_kw_minus_avg"),
        F.count(F.lit(1)).over(w_reading_id).alias("rows_in_reading_id"),
        F.ntile(3).over(w_all_by_reading_kwh).alias("reading_kwh_tercile"),
        F.round(F.col("reading_voltage") / F.nullif(F.sum("reading_voltage").over(w_reading_id), F.lit(0)), 4).alias("reading_voltage_share_of_reading_id"),
    )
)

result = (
    ranked
    .groupingSets(
        [["reading_kwh_tercile", "reading_id"], ["reading_kwh_tercile"]],
        "reading_kwh_tercile", "reading_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count("*").alias("row_count"),
        F.countDistinct("reading_id").alias("distinct_reading_ids"),
    )
    .orderBy(
        F.col("reading_kwh_tercile").asc_nulls_last(),
        F.col("reading_id").asc_nulls_last(),
    )
)
