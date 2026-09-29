from pyspark.sql import functions as F

matched = (
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

zone_stats = (
    matched
    .groupBy("zone")
    .agg(F.avg("energy_cost").alias("avg_energy_cost"))
)

above_average = (
    matched.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.energy_cost") > F.col("g.avg_energy_cost"))
    .select(
        "s.*",
        "g.avg_energy_cost",
        F.round(F.col("s.energy_cost") - F.col("g.avg_energy_cost"), 2).alias("energy_cost_minus_avg"),
    )
)

joined_site = (
    above_average.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "s.avg_energy_cost",
        "s.energy_cost_minus_avg",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    joined_site
    .filter(F.col("energy_cost") < 540.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "rate",
        "avg_energy_cost",
        "site_installed_kw",
        (F.floor(F.col("avg_energy_cost") / 250) * 250).alias("avg_energy_cost_bucket"),
        F.length(F.trim(F.col("zone"))).alias("zone_len"),
    )
    .orderBy(F.col("zone_len").asc(), F.col("avg_energy_cost_bucket").desc())
)
