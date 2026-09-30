from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.quality_clean",
        "s.last_reading_read_at",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

joined_site = (
    combined_details.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "TOU")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.site_installed_kw",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    joined_site
    .groupBy("site_zone", "read_at")
    .pivot("quality", ["EST", "BAD"])
    .agg(F.min("site_installed_kw"))
    .orderBy("site_zone", "read_at")
)
