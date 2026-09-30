from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")

prepared = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 237.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

with_details_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

ranked = (
    with_details_site
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "voltage_filled",
        "site_installed_kw",
        "site_name",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("voltage_per_kwh") - F.avg("voltage_per_kwh").over(w_site_id), 2).alias("voltage_per_kwh_vs_site_id_avg"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
    )
)

profiled = (
    ranked.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.last_reading_read_at_2",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

with_parent = (
    profiled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_filled",
        "s.site_installed_kw",
        "s.site_name",
        "s.rows_in_site_id",
        "s.last_reading_read_at",
        "s.last_reading_read_at_2",
        "s.reading_quality_count",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_parent_site = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.site_installed_kw",
        "s.site_name",
        "s.rows_in_site_id",
        "s.last_reading_read_at",
        "s.last_reading_read_at_2",
        "s.reading_quality_count",
        "s.site_tariff",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_parent_site
    .groupBy("site_tariff", "site_id")
    .pivot("quality", ["OK", "EST"])
    .agg(F.count("site_installed_kw"))
    .orderBy("site_tariff", "site_id")
)
