from pyspark.sql import functions as F

covered = (
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

zone_stats = (
    covered
    .groupBy("zone")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

compared = (
    covered.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

qualified = (
    compared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

combined_details = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isin("Z4", "Z2"))
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

result = (
    combined_details
    .filter(F.col("site_id") <= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "median_kwh",
        "kwh_minus_median",
        "site_zone",
        "site_tariff",
        "site_name",
        F.upper(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
    )
    .orderBy(F.col("site_zone").asc_nulls_last())
)
