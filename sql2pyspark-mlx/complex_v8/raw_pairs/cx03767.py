from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")) & (F.col("voltage") < 240.2))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    base_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

line_level = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

qualified = (
    line_level.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "BAD"), "left_semi")
)

stats = (
    qualified
    .groupBy(F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.array_sort(F.collect_set("reading_quality")).alias("reading_quality_list"),
        F.percentile_approx("reading_voltage", 0.5).alias("median_reading_voltage"),
    )
)

annotated = (
    stats
    .select(
        "read_at_day",
        "last_read_at",
        "median_reading_voltage",
        F.date_format(F.col("read_at_day"), "MMM yyyy").alias("read_at_day_label"),
    )
)

result = (
    annotated
    .select(
        "read_at_day",
        "last_read_at",
        "median_reading_voltage",
        "read_at_day_label",
    )
    .orderBy(F.col("median_reading_voltage").desc())
)
