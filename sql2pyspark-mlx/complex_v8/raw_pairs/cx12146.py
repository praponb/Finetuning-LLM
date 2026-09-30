from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 2)) & (F.col("quality").isin("OK", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

quality_stats = (
    recent
    .groupBy("quality")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

above_average = (
    recent.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

with_details = (
    above_average.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.kwh_filled",
        "s.avg_voltage",
        "s.voltage_minus_avg",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

screened = (
    with_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 81.3) & (F.hour(F.col("m.read_at")).between(6, 8)), "left_anti")
)

site_id_stats = (
    screened
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage_2"))
)

above_average_2 = (
    screened.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage_2"))
    .select(
        "s.*",
        "g.avg_voltage_2",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage_2"), 2).alias("voltage_minus_avg_2"),
    )
)

enriched = (
    above_average_2.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_avg",
        "s.site_zone",
        "s.site_name",
        "s.avg_voltage_2",
        "s.voltage_minus_avg_2",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.site_name").alias("site_site_name"),
    )
)

result = (
    enriched
    .select(
        "id",
        "voltage_minus_avg",
        "voltage_minus_avg_2",
        "voltage",
        "avg_voltage_2",
    )
    .unpivot(
        ids=["id"],
        values=["voltage_minus_avg", "voltage_minus_avg_2", "voltage", "avg_voltage_2"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
