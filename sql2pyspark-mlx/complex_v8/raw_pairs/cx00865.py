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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

quality_stats = (
    covered
    .groupBy("quality")
    .agg(F.max("rate").alias("max_rate"))
)

above_average = (
    covered.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.rate") == F.col("g.max_rate"))
    .select(
        "s.*",
        "g.max_rate",
        F.round(F.col("s.rate") - F.col("g.max_rate"), 2).alias("rate_minus_max"),
    )
)

enriched_site = (
    above_average.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.rate",
        "s.energy_cost",
        "s.rate_minus_max",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_parent_site = (
    enriched_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.energy_cost",
        "s.site_zone",
        "si.site_name",
    )
)

result = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "energy_cost",
        "site_zone",
        "site_name",
    )
    .orderBy(F.col("read_at").desc(), F.col("energy_cost").desc(), "id")
    .limit(5)
)
