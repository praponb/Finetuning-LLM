from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("voltage") <= 231.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

line_level_readings = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_slug",
        "s.quality_ref",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

detail_rows = (
    line_level_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_slug",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_voltage",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.voltage").alias("reading_voltage_2"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

result = (
    detail_rows
    .groupingSets(
        [["reading_id_2", "site_id"], ["reading_id_2"], []],
        "reading_id_2", "site_id",
    )
    .agg(F.count("*").alias("n_rows"))
)
