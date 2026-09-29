from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(6, 12)) | (F.col("voltage") < 237.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.abs(F.col("kwh") - 86.0).alias("kwh_distance"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

screened = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(45.0, 117.4)), "left_semi")
)

joined_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_slug",
        "s.kwh_distance",
        "s.voltage_plus_kwh",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

site_id_stats = (
    joined_site
    .groupBy("site_id")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

vs_group = (
    joined_site.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") > 15.9), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.quality_slug",
        "s.kwh_distance",
        "s.voltage_plus_kwh",
        "s.site_zone",
        "s.site_tariff",
        "s.avg_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

quality_stats = (
    with_children
    .groupBy("quality")
    .agg(F.percentile_approx("voltage_plus_kwh", 0.5).alias("median_voltage_plus_kwh"))
)

vs_group_2 = (
    with_children.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage_plus_kwh") >= F.col("g.median_voltage_plus_kwh"))
    .select(
        "s.*",
        "g.median_voltage_plus_kwh",
        F.round(F.col("s.voltage_plus_kwh") - F.col("g.median_voltage_plus_kwh"), 2).alias("voltage_plus_kwh_minus_median"),
    )
)

result = (
    vs_group_2
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "kwh_distance",
        "site_zone",
        "site_tariff",
        "avg_kwh",
        "reading_id",
        "reading_kwh",
        "reading_read_at",
        "median_voltage_plus_kwh",
        "voltage_plus_kwh_minus_median",
        F.upper(F.trim(F.col("site_tariff"))).alias("site_tariff_clean"),
    )
    .orderBy("site_zone", F.col("read_at").asc_nulls_last())
)
