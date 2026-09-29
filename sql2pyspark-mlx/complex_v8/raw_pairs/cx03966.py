from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

exploded_detail = (
    prepared.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

labeled = (
    exploded_detail
    .filter(F.col("kwh").between(30.2, 92.5))
    .select(
        "*",
        F.greatest(F.col("kwh"), F.col("reading_voltage")).alias("greatest_kwh_reading_voltage"),
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("id").cast("string")).alias("reading_quality_ref"),
    )
)

joined_reading = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.reading_id",
        "s.reading_voltage",
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.voltage").alias("reading_voltage_2"),
    )
)

result = (
    joined_reading
    .select("kwh", "reading_id")
    .distinct()
    .orderBy("kwh")
)
