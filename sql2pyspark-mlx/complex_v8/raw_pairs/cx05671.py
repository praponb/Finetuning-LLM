from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("site_id") >= 4)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

line_level = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(212.7, 233.1)), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    line_level.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .rollup("quality", "reading_count")
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.max("reading_read_at").alias("last_reading_read_at"),
        F.count(F.when(F.col("quality").isin("OK", "BAD"), 1)).alias("n_quality_in_ok_bad"),
    )
)
