from pyspark.sql import functions as F

covered = (
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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

without_matches = (
    covered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")), "left_anti")
)

screened = (
    without_matches.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(213.2, 240.1)), "left_anti")
)

result = (
    screened
    .select(
        "id",
        F.col("kwh").alias("kwh_v"),
        F.col("rate").alias("m_rate"),
        F.col("energy_cost").alias("m_energy_cost"),
    )
    .unpivot(
        ids=["id"],
        values=["kwh_v", "m_rate", "m_energy_cost"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
