from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("voltage") < 240.2) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

summarized_rows = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

grouped = (
    summarized_rows
    .groupBy("site_id")
    .agg(
        F.min("first_reading_read_at").alias("first_first_reading_read_at"),
        F.round(100 * F.count(F.when(~F.col("quality").isin("EST", "BAD"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_est_bad"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

exploded_detail_readings = (
    grouped.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.first_first_reading_read_at",
        "s.pct_quality_not_in_est_bad",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_details_reading = (
    exploded_detail_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .select(
        "s.site_id",
        "s.pct_quality_not_in_est_bad",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_kwh",
        "s.reading_quality",
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

enriched = (
    with_details_reading
    .select(
        "*",
        F.date_format(F.col("reading_read_at_2"), "yyyy-MM-dd HH:00").alias("reading_read_at_2_label"),
    )
)

result = (
    enriched
    .groupBy("reading_site_id")
    .pivot("reading_quality", ["BAD", "OK", "EST"])
    .agg(F.max("reading_voltage"))
    .orderBy("reading_site_id")
)
