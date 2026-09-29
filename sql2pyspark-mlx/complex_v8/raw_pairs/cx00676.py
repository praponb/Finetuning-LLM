from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("voltage") > 221.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at", "m.first_reading_read_at")
)

result = (
    summarized_rows
    .groupBy("last_reading_read_at")
    .pivot("quality", ["OK", "EST"])
    .agg(F.min("voltage"))
    .orderBy("last_reading_read_at")
)
