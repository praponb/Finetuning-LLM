from pyspark.sql import functions as F

scoped = (
    df_sites
    .filter((F.col("installed_kw") <= 386.6) & (F.col("tariff").isin("EV", "FLAT")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_add(F.col("commissioned_on"), 30).alias("commissioned_on_plus_30d"),
    )
)

zone_stats = (
    scoped
    .groupBy("zone")
    .agg(F.max("installed_kw").alias("max_installed_kw"))
)

outliers = (
    scoped.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.installed_kw") == F.col("g.max_installed_kw"))
    .select(
        "s.*",
        "g.max_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.max_installed_kw"), 2).alias("installed_kw_minus_max"),
    )
)

detail_rows_readings = (
    outliers.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.site_name",
        "s.commissioned_on",
        "s.commissioned_on_plus_30d",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    detail_rows_readings
    .select(
        "id",
        "site_name",
        "commissioned_on_plus_30d",
        "reading_kwh",
        "reading_voltage",
        "reading_read_at",
    )
    .orderBy(F.col("reading_read_at").asc())
)
