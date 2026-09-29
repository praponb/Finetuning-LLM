from pyspark.sql import functions as F

in_window = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
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

filtered = (
    in_window.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isin("FLAT", "EV")) & (F.col("si.installed_kw").between(96.9, 386.6)), "left_semi")
)

detail_rows = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(14.4, 110.5)), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "read_at",
        "voltage",
        "energy_cost",
        "reading_id",
        "reading_kwh",
        "reading_read_at",
        F.when(F.col("voltage") >= 238.3, "A").when(F.col("voltage") >= 220.7, "B").otherwise("C").alias("voltage_tier"),
    )
    .orderBy("id", F.col("reading_read_at").desc())
)
