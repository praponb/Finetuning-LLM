from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("voltage").between(219.6, 237.5)) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

screened = (
    recent_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    screened
    .groupBy("read_at")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.min("voltage"))
    .orderBy("read_at")
)
