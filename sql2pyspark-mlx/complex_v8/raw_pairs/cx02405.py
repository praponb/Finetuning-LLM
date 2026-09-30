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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

scored = (
    in_window
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.round(F.col("rate") / F.nullif(F.col("energy_cost"), F.lit(0)), 3).alias("rate_per_energy_cost"),
        F.concat(F.col("zone"), F.lit("#"), F.col("site_id").cast("string")).alias("zone_ref"),
    )
)

result = (
    scored
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        "zone_ref",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
    .orderBy("quality", "site_id")
)
