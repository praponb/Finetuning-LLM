from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.kwh_per_voltage",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

derived = (
    enriched
    .select(
        "id",
        "site_id",
        "kwh",
        "kwh_per_voltage",
        "site_name",
        "site_tariff",
        "site_zone",
        F.coalesce(F.col("kwh_per_voltage"), F.lit(0.0)).alias("kwh_per_voltage_filled"),
    )
)

result = (
    derived
    .groupBy("site_id", "site_tariff")
    .pivot("site_zone", ["Z2", "Z3"])
    .agg(F.sum("kwh_per_voltage_filled"))
)
