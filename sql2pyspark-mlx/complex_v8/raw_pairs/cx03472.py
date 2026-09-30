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

joined = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z2")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined
    .filter(~F.col("zone").isin("Z2", "Z4"))
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "rate",
        "energy_cost",
        "site_zone",
        "site_commissioned_on",
        F.lower(F.trim(F.col("zone"))).alias("zone_clean"),
    )
    .orderBy("zone_clean")
)
