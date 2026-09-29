from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(7, 13))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

with_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_ref",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

screened = (
    with_details.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.length(F.trim(F.col("si.site_name"))) >= 4) & (F.col("si.commissioned_on") < F.lit("2025-05-07").cast("date")), "left_semi")
)

with_activity = (
    screened.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone").isNotNull()) & (F.col("si.tariff").isNotNull()), "left_anti")
)

derived = (
    with_activity
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

result = (
    derived
    .select(
        "id",
        "read_at",
        "quality",
        "site_tariff",
        "site_name",
        "quality_or_unknown",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
