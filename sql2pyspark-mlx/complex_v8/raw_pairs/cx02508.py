from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

without_matches = (
    recent
    .crossJoin(recent.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

site_id_stats = (
    without_matches
    .groupBy("site_id")
    .agg(F.avg("read_at_quarter").alias("avg_read_at_quarter"))
)

outliers = (
    without_matches.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.read_at_quarter") > F.col("g.avg_read_at_quarter"))
    .select(
        "s.*",
        "g.avg_read_at_quarter",
        F.round(F.col("s.read_at_quarter") - F.col("g.avg_read_at_quarter"), 2).alias("read_at_quarter_minus_avg"),
    )
)

filtered = (
    outliers.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

with_details = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_per_kwh",
        "s.read_at_date",
        "s.avg_read_at_quarter",
        "s.read_at_quarter_minus_avg",
        F.col("si.zone").alias("site_zone"),
    )
)

screened = (
    with_details.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 66.5) & (F.col("si.commissioned_on").isNotNull()), "left_semi")
)

screened_2 = (
    screened.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(43.1, 85.7)), "left_semi")
    .filter(F.col("site_id").isNotNull())
)

result = (
    screened_2
    .select(
        "id",
        "site_id",
        "quality",
        "voltage_per_kwh",
        "avg_read_at_quarter",
        "read_at_quarter_minus_avg",
    )
    .orderBy(F.col("site_id").desc_nulls_first())
)
