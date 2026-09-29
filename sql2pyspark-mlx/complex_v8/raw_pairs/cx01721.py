from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage").between(220.7, 239.6)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

combined_details_site = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "si.site_name",
    )
)

result = (
    combined_details_site
    .groupBy("site_id", "read_at")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.count("kwh"))
    .orderBy("site_id", "read_at")
)
