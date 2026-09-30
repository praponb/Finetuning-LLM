from pyspark.sql import functions as F

base = (
    df_sites
    .filter(~F.col("tariff").isin("EV", "FLAT"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        F.substring(F.trim(F.col("zone")), 1, 4).alias("zone_prefix"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at_2"),
    )
)

with_stats_2 = (
    with_stats.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at_2",
    )
)

result = (
    with_stats_2
    .groupBy("tariff")
    .pivot("zone", ["Z3", "Z4", "Z2"])
    .agg(F.sum("reading_count"))
    .orderBy("tariff")
)
