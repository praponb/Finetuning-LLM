from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("voltage").between(213.2, 240.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_day",
        "s.read_at_date",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

site_id_stats = (
    combined_details
    .groupBy("site_id")
    .agg(F.max("site_installed_kw").alias("max_site_installed_kw"))
)

compared = (
    combined_details.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.site_installed_kw") == F.col("g.max_site_installed_kw"))
    .select(
        "s.*",
        "g.max_site_installed_kw",
        F.round(F.col("s.site_installed_kw") - F.col("g.max_site_installed_kw"), 2).alias("site_installed_kw_minus_max"),
    )
)

joined = (
    compared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.read_at_day",
        "s.site_zone",
        "s.site_installed_kw",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

result = (
    joined
    .filter(F.length(F.trim(F.col("site_name"))) >= 7)
    .select(
        "id",
        "read_at",
        "quality",
        "read_at_day",
        "site_name",
        "site_commissioned_on",
        (F.col("site_installed_kw_2") + F.col("site_installed_kw")).alias("site_installed_kw_2_plus_site_installed_kw"),
    )
    .orderBy(F.col("read_at").desc())
)
