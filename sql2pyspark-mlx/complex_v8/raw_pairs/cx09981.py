from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("kwh") > 35.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

without_matches = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.voltage") >= 223.3), "left_anti")
)

quality_stats = (
    without_matches
    .groupBy("quality")
    .agg(F.percentile_approx("read_at_quarter", 0.5).alias("median_read_at_quarter"))
)

outliers = (
    without_matches.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.read_at_quarter") >= F.col("g.median_read_at_quarter"))
    .select(
        "s.*",
        "g.median_read_at_quarter",
        F.round(F.col("s.read_at_quarter") - F.col("g.median_read_at_quarter"), 2).alias("read_at_quarter_minus_median"),
    )
)

filtered = (
    outliers.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-07").cast("date"))) & (F.col("si.installed_kw") < 284.0), "left_semi")
)

result = (
    filtered
    .filter(F.col("read_at_quarter") <= 3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_quarter",
        "median_read_at_quarter",
        "read_at_quarter_minus_median",
        F.abs(F.col("voltage") - 223.3).alias("voltage_distance"),
    )
    .orderBy("quality")
)
