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

enriched = (
    matched
    .select(
        "id",
        "site_id",
        "rate",
        "energy_cost",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
        F.greatest(F.col("energy_cost"), F.col("voltage")).alias("greatest_energy_cost_voltage"),
    )
)

result = (
    enriched
    .select(
        "id",
        "rate",
        "energy_cost",
        "read_at_quarter",
        "greatest_energy_cost_voltage",
        F.round(F.col("rate") * 1.07, 2).alias("rate_adj"),
    )
    .orderBy(
        F.col("energy_cost").desc_nulls_first(),
        F.col("rate_adj").desc_nulls_first(),
    )
)
