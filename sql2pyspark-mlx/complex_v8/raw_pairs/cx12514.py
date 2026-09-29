from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("voltage").between(219.6, 233.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

site_id_stats = (
    base_readings
    .groupBy("site_id")
    .agg(F.avg("greatest_kwh_voltage").alias("avg_greatest_kwh_voltage"))
)

outliers = (
    base_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.greatest_kwh_voltage") > F.col("g.avg_greatest_kwh_voltage"))
    .select(
        "s.*",
        "g.avg_greatest_kwh_voltage",
        F.round(F.col("s.greatest_kwh_voltage") - F.col("g.avg_greatest_kwh_voltage"), 2).alias("greatest_kwh_voltage_minus_avg"),
    )
)

enriched = (
    outliers.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.greatest_kwh_voltage",
        "s.avg_greatest_kwh_voltage",
        "s.greatest_kwh_voltage_minus_avg",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    enriched
    .cube("quality", "site_zone", "site_id")
    .agg(F.array_sort(F.collect_set("quality")).alias("quality_list"))
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        "site_id",
        "quality_list",
    )
)
