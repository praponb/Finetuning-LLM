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

detail_rows = (
    in_window.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.zone",
        "s.energy_cost",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "site_id",
        "voltage",
        "zone",
        "reading_id",
        "reading_read_at",
        "reading_voltage",
        "reading_kwh",
        F.round(F.col("reading_kwh") / F.nullif(F.col("energy_cost"), F.lit(0)), 3).alias("reading_kwh_per_energy_cost"),
        F.coalesce(F.col("reading_quality"), F.lit("UNKNOWN")).alias("reading_quality_or_unknown"),
    )
    .orderBy(
        F.col("reading_quality_or_unknown").desc_nulls_first(),
        F.col("zone").asc_nulls_last(),
        "id",
        "reading_id",
    )
    .limit(10)
)
