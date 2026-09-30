from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("kwh") <= 109.3))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

exploded_detail = (
    cleaned_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh").between(30.2, 81.1)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

derived = (
    exploded_detail
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("reading_read_at"))) / 3600, 1).alias("hours_reading_read_at_to_read_at"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("reading_id").cast("string")).alias("reading_quality_ref"),
    )
)

result = (
    derived
    .select(
        "id",
        "site_id",
        "quality",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        "reading_kwh",
        "reading_voltage",
        "hours_reading_read_at_to_read_at",
        "reading_quality_ref",
        F.round(F.col("reading_kwh") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("reading_kwh_per_kwh"),
    )
    .orderBy(F.col("reading_quality_ref").desc_nulls_first())
)
