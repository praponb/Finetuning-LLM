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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

tagged = (
    covered
    .filter(F.col("energy_cost") < 765.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

result = (
    tagged
    .select("id", "rate", "kwh")
    .unpivot(
        ids=["id"],
        values=["rate", "kwh"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
