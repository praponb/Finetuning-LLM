from pyspark.sql import functions as F

joined_ranges = (
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

enriched = (
    joined_ranges
    .filter(F.col("rate") > 3.0)
    .select(
        "*",
        F.abs(F.col("voltage") - 226.0).alias("voltage_distance"),
        (F.floor(F.col("energy_cost") / 250) * 250).alias("energy_cost_bucket"),
    )
)

filtered = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(219.6, 232.0)) & (F.col("m.kwh") <= 84.9), "left_anti")
    .filter(F.col("energy_cost") >= 180.0)
)

with_parent = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.zone",
        "s.energy_cost",
        "s.voltage_distance",
        "s.energy_cost_bucket",
        "si.site_name",
    )
)

result = (
    with_parent
    .filter(F.col("voltage_distance") >= 73.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "energy_cost",
        "voltage_distance",
        "energy_cost_bucket",
        "site_name",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        (F.col("voltage_distance") - F.col("energy_cost")).alias("voltage_distance_minus_energy_cost"),
    )
    .orderBy(F.col("read_at").desc())
)
