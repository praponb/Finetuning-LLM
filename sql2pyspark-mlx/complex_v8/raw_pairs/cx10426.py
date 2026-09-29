from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 113.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

exploded_detail = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_prefix",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"))
)

with_counts = (
    exploded_detail.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_kwh")
)

result = (
    with_counts
    .groupingSets(
        [["reading_id", "site_id"], ["reading_id"], ["site_id"]],
        "reading_id", "site_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("kwh") / F.nullif(F.sum("reading_voltage"), F.lit(0)), 3).alias("kwh_to_reading_voltage_ratio"),
        F.round(F.sum("reading_kwh") / F.nullif(F.sum("reading_voltage"), F.lit(0)), 3).alias("reading_kwh_to_reading_voltage_ratio"),
    )
)
