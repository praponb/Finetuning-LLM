from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 216.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

with_details = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "FLAT"), "left")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

enriched = (
    with_details
    .filter(F.col("read_at") >= F.lit("2025-01-19 12:00:00").cast("timestamp"))
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "site_tariff",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

derived = (
    enriched
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "site_tariff",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled_2"),
        (F.col("voltage") - F.col("voltage_filled")).alias("voltage_minus_voltage_filled"),
        F.length(F.trim(F.col("site_tariff"))).alias("site_tariff_len"),
    )
)

screened = (
    derived
    .crossJoin(derived.agg(F.min("voltage").alias("overall_min_voltage")))
    .filter(F.col("voltage") > F.col("overall_min_voltage"))
    .drop("overall_min_voltage")
)

result = (
    screened
    .filter(F.col("quality") != "OK")
    .select(
        "id",
        "voltage",
        "quality",
        "site_tariff",
        "voltage_filled_2",
        "voltage_minus_voltage_filled",
        "site_tariff_len",
    )
    .orderBy(F.col("site_tariff_len").asc_nulls_last())
)
