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

scored = (
    covered
    .select(
        "*",
        F.least(F.col("rate"), F.col("energy_cost")).alias("least_rate_energy_cost"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

result = (
    scored
    .select(
        "id",
        F.col("kwh").alias("kwh_v"),
        F.col("rate").alias("m_rate"),
        F.col("energy_cost").alias("energy_cost_v"),
        F.col("least_rate_energy_cost").alias("m_least_rate_energy_cost"),
    )
    .unpivot(
        ids=["id"],
        values=["kwh_v", "m_rate", "energy_cost_v", "m_least_rate_energy_cost"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
