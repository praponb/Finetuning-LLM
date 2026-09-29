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

site_id_stats = (
    covered
    .groupBy("site_id")
    .agg(F.avg("energy_cost").alias("avg_energy_cost"))
)

above_average = (
    covered.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.energy_cost") > F.col("g.avg_energy_cost"))
    .select(
        "s.*",
        "g.avg_energy_cost",
        F.round(F.col("s.energy_cost") - F.col("g.avg_energy_cost"), 2).alias("energy_cost_minus_avg"),
    )
)

joined_site = (
    above_average.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.zone",
        "s.energy_cost",
        "s.energy_cost_minus_avg",
        "si.site_name",
    )
)

enriched = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") < 284.0), "left")
    .select(
        "s.*",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    enriched
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "energy_cost",
        "site_name",
        "site_tariff",
        "site_zone",
        F.concat_ws("|", F.col("site_zone"), F.col("site_tariff")).alias("site_zone_site_tariff_label"),
    )
    .orderBy("site_id")
)
