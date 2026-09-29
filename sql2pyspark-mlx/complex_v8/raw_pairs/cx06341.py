from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("reading_quality", "tariff").orderBy(F.col("reading_voltage").desc(), F.col("id").desc(), F.col("reading_id").desc())

base = (
    df_sites
    .filter(F.col("installed_kw") < 284.0)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

with_children_readings = (
    base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.site_name",
        "s.zone",
        "s.tariff",
        "s.installed_kw",
        "s.zone_or_unknown",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

latest = (
    with_children_readings
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details_reading = (
    latest.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("m.site_id"), "inner")
    .select(
        "s.id",
        "s.site_name",
        "s.zone",
        "s.installed_kw",
        "s.zone_or_unknown",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_quality",
        "s.reading_kwh",
        F.col("m.kwh").alias("reading_kwh_2"),
        F.col("m.site_id").alias("reading_site_id"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details_reading
    .filter(F.col("site_zone").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "reading_voltage",
        "reading_quality",
        "reading_kwh",
        "reading_kwh_2",
        "site_zone",
        "site_installed_kw",
        F.coalesce(F.col("reading_quality"), F.lit("UNKNOWN")).alias("reading_quality_or_unknown"),
    )
    .orderBy("zone", F.col("reading_quality").desc())
)
