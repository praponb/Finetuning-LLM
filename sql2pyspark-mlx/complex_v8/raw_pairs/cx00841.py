from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 14))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

enriched_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") < 284.0)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.voltage_minus_kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

line_level = (
    enriched_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.voltage_minus_kwh",
        "s.site_installed_kw",
        "s.site_name",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

tagged = (
    line_level
    .select(
        "id",
        "site_id",
        "quality",
        "voltage_minus_kwh",
        "site_installed_kw",
        "site_name",
        "reading_id",
        "reading_quality",
        "reading_voltage",
        F.round(F.col("reading_voltage") / F.nullif(F.col("reading_kwh"), F.lit(0)), 3).alias("reading_voltage_per_reading_kwh"),
    )
)

result = (
    tagged
    .rollup("quality", "reading_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("reading_quality") == "OK", F.col("reading_voltage_per_reading_kwh"))).alias("reading_voltage_per_reading_kwh_reading_quality_"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "reading_id",
        "grouping_level",
        "cnt",
        "reading_voltage_per_reading_kwh_reading_quality_",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("reading_id").asc_nulls_last())
)
