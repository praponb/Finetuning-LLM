from pyspark.sql import functions as F

in_window = (
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

exploded_detail_readings = (
    in_window.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 79.7), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.rate",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

labeled = (
    exploded_detail_readings
    .select(
        "id",
        "site_id",
        "voltage",
        "rate",
        "energy_cost",
        "reading_id",
        "reading_kwh",
        F.round(F.col("kwh") / F.nullif(F.col("reading_voltage"), F.lit(0)), 3).alias("kwh_per_reading_voltage"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "voltage",
        "energy_cost",
        "reading_kwh",
    )
    .orderBy(F.col("voltage").desc(), F.col("id").asc_nulls_last())
)
