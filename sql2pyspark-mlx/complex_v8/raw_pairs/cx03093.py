from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 45.3) & (F.hour(F.col("read_at")).between(7, 13)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

with_parent = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isNotNull()), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_per_kwh",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

annotated = (
    with_parent
    .select(
        "*",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

with_details_site = (
    annotated.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.site_tariff",
        "s.site_zone",
        "s.read_at_month",
        "s.kwh_plus_voltage",
        "s.read_at_label",
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

combined_details = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") != "EV")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.site_tariff",
        "s.site_zone",
        "s.kwh_plus_voltage",
        "s.read_at_label",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

site_id_stats = (
    combined_details
    .groupBy("site_id")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

vs_group = (
    combined_details.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

with_details_site_2 = (
    vs_group.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.site_zone",
        "s.read_at_label",
        "s.site_installed_kw_2",
        "s.site_tariff_2",
        "s.avg_kwh",
        F.col("si.tariff").alias("site_tariff_3"),
        F.col("si.zone").alias("site_zone_2"),
    )
)

result = (
    with_details_site_2
    .select("id", "avg_kwh", "site_installed_kw_2")
    .unpivot(
        ids=["id"],
        values=["avg_kwh", "site_installed_kw_2"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric_name")
)
