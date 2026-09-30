from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-02 13:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

combined_details_site = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z2")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.reading_count",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    combined_details_site
    .groupBy("read_at", "reading_count")
    .pivot("site_zone", ["Z4", "Z3", "Z2"])
    .agg(F.avg("kwh").alias("avg_v"), F.max("kwh").alias("max_v"))
    .orderBy("read_at", "reading_count")
)
