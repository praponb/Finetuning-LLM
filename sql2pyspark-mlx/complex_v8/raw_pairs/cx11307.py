from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.when(F.col("voltage") >= 238.8, "large").when(F.col("voltage") >= 218.2, "medium").otherwise("small").alias("voltage_tier"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.least(F.col("voltage"), F.col("kwh")).alias("least_voltage_kwh"),
    )
)

voltage_tier_stats = (
    cleaned_readings
    .groupBy("voltage_tier")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

compared = (
    cleaned_readings.alias("s")
    .join(voltage_tier_stats.alias("g"), F.col("g.voltage_tier") == F.col("s.voltage_tier"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

combined_details = (
    compared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.kwh_filled",
        "s.least_voltage_kwh",
        "s.avg_kwh",
        "s.kwh_minus_avg",
        F.col("si.tariff").alias("site_tariff"),
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 87.1) & (F.col("m.read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("site_tariff") == "FLAT")
)

result = (
    filtered
    .filter(F.col("voltage") >= 224.8)
    .select(
        "id",
        "read_at",
        "voltage",
        "least_voltage_kwh",
        "avg_kwh",
        "kwh_minus_avg",
        "site_tariff",
    )
    .orderBy(F.col("id").asc(), F.col("site_tariff").desc())
)
