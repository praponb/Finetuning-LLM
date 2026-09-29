from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 220.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.site_name",
        "s.site_commissioned_on",
        "s.site_tariff",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
    )
)

site_tariff_stats = (
    combined_details
    .groupBy("site_tariff")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

above_average = (
    combined_details.alias("s")
    .join(site_tariff_stats.alias("g"), F.col("g.site_tariff") == F.col("s.site_tariff"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

labeled = (
    above_average
    .filter(F.upper(F.trim(F.col("site_name"))).like("M%"))
    .select(
        "id",
        "read_at",
        "kwh",
        "site_name",
        "site_commissioned_on",
        "site_tariff",
        "site_commissioned_on_2",
        "median_kwh",
        "kwh_minus_median",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("site_commissioned_on_2")).alias("days_since_site_commissioned_on_2"),
        F.round(F.col("kwh") / F.nullif(F.col("median_kwh"), F.lit(0)), 3).alias("kwh_per_median_kwh"),
        F.greatest(F.col("median_kwh"), F.col("kwh_minus_median")).alias("greatest_median_kwh_kwh_minus_median"),
    )
)

result = (
    labeled
    .filter(F.col("days_since_site_commissioned_on_2") < 420)
    .select(
        "id",
        "read_at",
        "kwh",
        "site_name",
        "site_commissioned_on",
        "site_tariff",
        "site_commissioned_on_2",
        "kwh_minus_median",
        "days_since_site_commissioned_on_2",
        "kwh_per_median_kwh",
        "greatest_median_kwh_kwh_minus_median",
        F.abs(F.col("kwh_minus_median") - 36.0).alias("kwh_minus_median_distance"),
        F.round(F.col("median_kwh") * 0.25, 2).alias("median_kwh_adj"),
    )
    .orderBy(F.col("site_tariff").desc())
)
