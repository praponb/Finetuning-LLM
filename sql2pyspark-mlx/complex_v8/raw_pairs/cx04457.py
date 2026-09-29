from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 233.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

detail_rows_readings = (
    recent_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") <= 233.6), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality_clean",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

summarized_rows = (
    detail_rows_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

combined_details_reading = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .filter(F.hour(F.col("m.read_at")).between(8, 12))
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_voltage",
        "s.first_reading_read_at",
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    combined_details_reading
    .groupBy("reading_quality", "site_id")
    .pivot("reading_quality_2", ["BAD", "EST", "OK"])
    .agg(F.sum("reading_kwh"))
    .orderBy("reading_quality", "site_id")
)
