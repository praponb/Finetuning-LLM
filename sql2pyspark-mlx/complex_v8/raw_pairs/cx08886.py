from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group_2"),
    )
)

filtered = (
    base
    .crossJoin(base.agg(F.min("kwh").alias("overall_min_kwh")))
    .filter(F.col("kwh") > F.col("overall_min_kwh"))
    .drop("overall_min_kwh")
)

quality_stats = (
    filtered
    .groupBy("quality")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

vs_group = (
    filtered.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

joined = (
    vs_group.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_group",
        "s.quality_group_2",
        "s.avg_voltage",
        "s.voltage_minus_avg",
        F.col("si.tariff").alias("site_tariff"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

summarized_rows = (
    joined.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_group",
        "quality_group_2",
        "avg_voltage",
        (F.col("avg_voltage") + F.col("voltage_minus_avg")).alias("avg_voltage_plus_voltage_minus_avg"),
    )
    .orderBy(F.col("id").desc(), "read_at")
)
