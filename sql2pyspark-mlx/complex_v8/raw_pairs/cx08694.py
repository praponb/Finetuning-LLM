from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

with_parent_site = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        "s.read_at_month",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_parent_site
    .groupingSets(
        [["site_id", "site_tariff", "quality"], ["site_id"]],
        "site_id", "site_tariff", "quality",
    )
    .agg(
        F.sum(F.when(F.col("site_tariff").isin("FLAT", "EV"), F.col("read_at_month")).otherwise(0)).alias("read_at_month_site_tariff_in_flat_ev"),
        F.round(F.sum("site_installed_kw") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("site_installed_kw_to_voltage_ratio"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("site_tariff").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
