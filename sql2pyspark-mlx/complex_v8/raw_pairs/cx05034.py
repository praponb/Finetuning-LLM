from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("voltage") < 233.8) & (F.col("quality").isin("EST", "OK")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

filtered = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 237.5), "left_anti")
)

with_parent_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(~F.col("si.tariff").isin("TOU", "EV"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.reading_count",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_parent_site
    .rollup("site_tariff", "site_id", "reading_count")
    .agg(
        F.round(F.stddev("kwh"), 2).alias("stddev_kwh"),
        F.round(100 * F.count(F.when(F.col("quality").isin("OK", "BAD"), 1)) / F.count("*"), 1).alias("pct_quality_in_ok_bad"),
    )
    .select(
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "site_id",
        "reading_count",
        "stddev_kwh",
        "pct_quality_in_ok_bad",
    )
)
