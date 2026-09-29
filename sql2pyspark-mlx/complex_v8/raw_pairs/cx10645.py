from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("kwh") > 14.4) & (F.col("quality") == "EST"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.month(F.col("read_at")).alias("read_at_month"),
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"))
)

profiled = (
    cleaned.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_kwh")
)

without_matches = (
    profiled.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("R%")), "left_semi")
)

quality_group_stats = (
    without_matches
    .groupBy("quality_group")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

vs_group = (
    without_matches.alias("s")
    .join(quality_group_stats.alias("g"), F.col("g.quality_group") == F.col("s.quality_group"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

result = (
    vs_group
    .filter(F.col("kwh") > 33.9)
    .select(
        "id",
        "site_id",
        "kwh",
        "read_at_month",
        "quality_group",
        "avg_reading_kwh",
        "avg_voltage",
        "voltage_minus_avg",
        F.round(F.col("avg_reading_kwh") / F.nullif(F.col("read_at_month"), F.lit(0)), 3).alias("avg_reading_kwh_per_read_at_month"),
    )
)
