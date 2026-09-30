from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 221.8).alias("voltage_distance"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

labeled = (
    recent_readings
    .select(
        "*",
        F.least(F.col("kwh"), F.col("voltage_distance")).alias("least_kwh_voltage_distance"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.tariff").isin("EV", "TOU")) & (F.col("si.installed_kw").between(93.3, 373.3)), "left_semi")
)

filtered = (
    qualified.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") != "Z2"), "left_semi")
)

exploded_detail_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(37.1, 87.1)), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.voltage_distance",
        "s.read_at_hour",
        "s.least_kwh_voltage_distance",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

exploded_detail = (
    exploded_detail_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(35.0, 118.3)), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.read_at_hour",
        "s.reading_id",
        "s.reading_kwh",
        "s.reading_voltage",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "reading_id",
        F.col("reading_voltage").alias("m_reading_voltage"),
        F.col("reading_kwh").alias("m_reading_kwh"),
        F.col("kwh").alias("m_kwh"),
        F.col("reading_kwh_2").alias("m_reading_kwh_2"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["m_reading_voltage", "m_reading_kwh", "m_kwh", "m_reading_kwh_2"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
