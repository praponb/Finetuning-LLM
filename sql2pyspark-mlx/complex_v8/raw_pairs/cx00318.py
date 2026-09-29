from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

qualified = (
    matched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (~F.col("m.quality").isin("BAD", "OK")) & (F.col("m.kwh") > 51.5), "left_anti")
)

detail_rows_readings = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

scored = (
    detail_rows_readings
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at_2"),
        F.round(F.col("reading_kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("reading_kwh_per_voltage"),
    )
)

result = (
    scored
    .filter(F.col("days_since_read_at_2").between(210, 595))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "reading_kwh",
        "reading_voltage",
        "days_since_read_at",
        "reading_kwh_per_voltage",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.abs(F.col("reading_voltage") - 224.9).alias("reading_voltage_distance"),
    )
    .orderBy("id", "site_id")
)
