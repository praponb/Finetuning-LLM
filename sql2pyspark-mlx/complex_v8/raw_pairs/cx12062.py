from pyspark.sql import functions as F

scoped_readings = df_meter_readings

tagged = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

with_parent = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_prefix",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    with_parent
    .groupBy("site_commissioned_on", "read_at")
    .pivot("quality", ["OK", "EST", "BAD"])
    .agg(F.min("site_installed_kw"))
)
