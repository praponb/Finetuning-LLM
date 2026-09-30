from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") | (F.col("voltage") > 221.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

quality_stats = (
    prepared
    .groupBy("quality")
    .agg(F.max("voltage").alias("max_voltage"))
)

above_average = (
    prepared.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

screened = (
    above_average.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_counts = (
    screened.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_voltage", "m.last_reading_read_at")
)

result = (
    with_counts
    .select(
        "id",
        "quality",
        "read_at_month",
        "max_voltage",
        "total_reading_voltage",
        "last_reading_read_at",
    )
)
