from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("site_id") <= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

combined_details_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

combined_details = (
    combined_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    combined_details
    .filter(F.col("quality").isin("BAD", "OK"))
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "site_zone",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
    .orderBy(F.col("quality").desc(), F.col("id").asc())
)
