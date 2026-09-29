from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("kwh") >= 37.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    cleaned.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("kwh").alias("total_reading_kwh"))
)

profiled_2 = (
    profiled.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh")
)

result = (
    profiled_2
    .groupBy("site_id", "read_at")
    .pivot("quality", ["EST", "BAD"])
    .agg(F.sum("total_reading_kwh"))
    .orderBy("site_id", "read_at")
)
