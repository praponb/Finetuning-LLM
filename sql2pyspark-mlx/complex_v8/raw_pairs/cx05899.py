from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 35.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
    )
)

exploded_detail_readings = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 233.6), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.quality_prefix",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

scored = (
    exploded_detail_readings
    .filter(F.col("reading_read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reading_read_at")).alias("days_since_reading_read_at"),
    )
)

enriched = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.*",
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.voltage").alias("reading_voltage_2"),
    )
)

result = (
    enriched
    .groupBy("reading_read_at")
    .pivot("quality", ["EST", "BAD"])
    .agg(F.avg("reading_kwh"))
    .orderBy("reading_read_at")
)
