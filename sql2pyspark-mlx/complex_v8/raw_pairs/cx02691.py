from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("kwh") < 96.8) & (F.col("site_id") <= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

quality_or_unknown_stats = (
    cleaned
    .groupBy("quality_or_unknown")
    .agg(F.percentile_approx("voltage", 0.5).alias("median_voltage"))
)

outliers = (
    cleaned.alias("s")
    .join(quality_or_unknown_stats.alias("g"), F.col("g.quality_or_unknown") == F.col("s.quality_or_unknown"), "inner")
    .filter(F.col("s.voltage") >= F.col("g.median_voltage"))
    .select(
        "s.*",
        "g.median_voltage",
        F.round(F.col("s.voltage") - F.col("g.median_voltage"), 2).alias("voltage_minus_median"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    outliers.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
    )
)

labeled = (
    summarized_rows
    .withColumn("quality_or_unknown_len", F.length(F.trim(F.col("quality_or_unknown"))))
)

result = (
    labeled
    .groupBy("reading_count", "read_at")
    .pivot("quality_or_unknown", ["UNKNOWN", "BAD", "EST", "OK"])
    .agg(F.avg("quality_or_unknown_len").alias("avg_v"), F.count("quality_or_unknown_len").alias("n"))
)
