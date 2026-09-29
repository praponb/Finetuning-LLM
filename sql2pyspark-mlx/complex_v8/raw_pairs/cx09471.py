from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 89.8) & (F.to_date(F.col("m.read_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
)

with_children_readings = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 239.6), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality_ref",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    with_children_readings
    .groupBy("site_id", "reading_read_at")
    .pivot("reading_quality", ["EST", "BAD", "OK"])
    .agg(F.sum("reading_voltage"))
)
