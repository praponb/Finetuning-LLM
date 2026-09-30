from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("site_id") <= 3) & (F.col("voltage").between(220.7, 233.1)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp")), "left_semi")
)

screened = (
    filtered_2.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z4") & (F.col("si.installed_kw") > 141.6), "left_semi")
)

filtered_3 = (
    screened.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(6, 12)) & (F.col("m.kwh") >= 48.2), "left_anti")
)

joined = (
    filtered_3.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined
    .select("id", F.col("kwh").alias("m_kwh"), F.col("voltage").alias("m_voltage"))
    .unpivot(
        ids=["id"],
        values=["m_kwh", "m_voltage"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
