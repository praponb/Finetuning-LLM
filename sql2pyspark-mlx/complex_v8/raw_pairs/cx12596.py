from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 12)) & (F.col("kwh") >= 15.9))
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

line_level = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.quality_or_unknown",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_counts = (
    line_level.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

line_level_2 = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.quality_or_unknown",
        "s.reading_id",
        "s.reading_voltage",
        "s.last_reading_read_at",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage_2"),
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    line_level_2
    .groupingSets(
        [["quality_or_unknown", "reading_quality_2"], ["quality_or_unknown"], []],
        "quality_or_unknown", "reading_quality_2",
    )
    .agg(
        F.round(F.sum("kwh") / F.nullif(F.sum("reading_kwh"), F.lit(0)), 3).alias("kwh_to_reading_kwh_ratio"),
    )
    .select(
        F.coalesce(F.col("quality_or_unknown"), F.lit("ALL")).alias("quality_or_unknown"),
        F.coalesce(F.col("reading_quality_2"), F.lit("ALL")).alias("reading_quality_2"),
        "kwh_to_reading_kwh_ratio",
    )
)
