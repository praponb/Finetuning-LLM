from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(6, 12)) & (F.col("voltage") <= 238.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

exploded_detail_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.greatest_kwh_voltage",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

quality_stats = (
    exploded_detail_readings
    .groupBy("quality")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

vs_group = (
    exploded_detail_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

enriched_reading = (
    vs_group.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (F.col("m.voltage").between(220.2, 239.6)), "left")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.greatest_kwh_voltage",
        "s.reading_id",
        "s.reading_read_at",
        "s.avg_kwh",
        "s.kwh_minus_avg",
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh_2"),
        F.col("si.zone").alias("site_zone"),
    )
)

qualified = (
    enriched_reading
    .crossJoin(enriched_reading.agg(F.avg("kwh_minus_avg").alias("overall_avg_kwh_minus_avg")))
    .filter(F.col("kwh_minus_avg") > F.col("overall_avg_kwh_minus_avg"))
    .drop("overall_avg_kwh_minus_avg")
)

site_id_stats = (
    qualified
    .groupBy("site_id")
    .agg(F.avg("kwh_minus_avg").alias("avg_kwh_minus_avg"))
)

outliers = (
    qualified.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh_minus_avg") > F.col("g.avg_kwh_minus_avg"))
    .select(
        "s.*",
        "g.avg_kwh_minus_avg",
        F.round(F.col("s.kwh_minus_avg") - F.col("g.avg_kwh_minus_avg"), 2).alias("kwh_minus_avg_minus_avg"),
    )
)

per_group = (
    outliers
    .filter(F.col("reading_quality") == "OK")
    .groupBy(F.year("read_at").alias("read_at_year"), "site_zone")
    .agg(
        F.sum(F.when(F.col("site_zone") == "Z4", F.col("reading_kwh_2")).otherwise(0)).alias("reading_kwh_2_site_zone_z4"),
        F.count(F.when(F.col("site_zone").isNotNull(), 1)).alias("n_site_zone_is_not_null"),
        F.round(F.sum("kwh_minus_avg_minus_avg") / F.nullif(F.sum("reading_voltage"), F.lit(0)), 3).alias("kwh_minus_avg_minus_avg_to_reading_voltage_ratio"),
    )
)

result = (
    per_group
    .select(
        "read_at_year",
        "site_zone",
        "reading_kwh_2_site_zone_z4",
        "n_site_zone_is_not_null",
        "kwh_minus_avg_minus_avg_to_reading_voltage_ratio",
    )
    .orderBy(F.col("reading_kwh_2_site_zone_z4").desc(), "read_at_year", "site_zone")
    .limit(20)
)
