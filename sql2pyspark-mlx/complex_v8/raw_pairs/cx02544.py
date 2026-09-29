from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_details = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.voltage_adj",
        "s.quality_clean",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
    )
)

enriched = (
    with_details
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "voltage_adj",
        "quality_clean",
        "site_installed_kw",
        "site_name",
        F.length(F.trim(F.col("quality_clean"))).alias("quality_clean_len"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage_adj"), F.lit(0)), 3).alias("kwh_per_voltage_adj"),
        F.concat_ws("|", F.col("quality"), F.col("quality_clean")).alias("quality_quality_clean_label"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.voltage_adj",
        "s.quality_clean",
        "s.site_installed_kw",
        "s.quality_clean_len",
        "s.kwh_per_voltage_adj",
        "s.quality_quality_clean_label",
        F.col("si.zone").alias("site_zone"),
        F.col("si.site_name").alias("site_site_name"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "site_id",
        "quality",
        "voltage_adj",
        "quality_clean",
        "kwh_per_voltage_adj",
        "site_zone",
        "site_site_name",
    )
    .orderBy(
        F.col("kwh_per_voltage_adj").desc_nulls_first(),
        F.col("site_zone").desc_nulls_first(),
    )
)
