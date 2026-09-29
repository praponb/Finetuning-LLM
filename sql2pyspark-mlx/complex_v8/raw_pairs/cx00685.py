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

enriched_site = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.zone",
        "s.rate",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

without_matches = (
    enriched_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD") & (F.month(F.col("m.read_at")).isin(2, 3)), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "site_commissioned_on",
    )
    .orderBy(F.col("site_id").desc_nulls_first())
)
