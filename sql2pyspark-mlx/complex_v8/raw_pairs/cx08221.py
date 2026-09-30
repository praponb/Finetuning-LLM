from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 79.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.when(F.col("voltage") >= 232.4, "large").when(F.col("voltage") >= 224.9, "medium").otherwise("small").alias("voltage_tier"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_counts = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_voltage", "m.last_reading_read_at")
)

quality_group_stats = (
    with_counts
    .groupBy("quality_group")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

outliers = (
    with_counts.alias("s")
    .join(quality_group_stats.alias("g"), F.col("g.quality_group") == F.col("s.quality_group"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "kwh_per_voltage",
        "voltage_tier",
        "min_reading_voltage",
        "last_reading_read_at",
        "avg_voltage",
        F.concat(F.col("voltage_tier"), F.lit("#"), F.col("site_id").cast("string")).alias("voltage_tier_ref"),
    )
)
