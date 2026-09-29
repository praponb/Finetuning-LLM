from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("kwh") > 48.2) & (F.col("quality").isin("OK", "BAD")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

exploded_detail_readings = (
    base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.quality_clean",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

tagged = (
    exploded_detail_readings
    .select(
        "*",
        F.substring(F.trim(F.col("reading_quality")), 1, 4).alias("reading_quality_prefix"),
        F.abs(F.col("reading_voltage") - 227.1).alias("reading_voltage_distance"),
        F.coalesce(F.col("reading_voltage"), F.lit(0.0)).alias("reading_voltage_filled"),
    )
)

result = (
    tagged
    .groupingSets(
        [["site_id", "quality", "reading_quality"], ["site_id"], ["quality"], []],
        "site_id", "quality", "reading_quality",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("reading_voltage_filled") / F.nullif(F.sum("reading_voltage_distance"), F.lit(0)), 3).alias("reading_voltage_filled_to_reading_voltage_distan"),
        F.min("reading_voltage_filled").alias("min_reading_voltage_filled"),
        F.count("*").alias("cnt"),
    )
)
