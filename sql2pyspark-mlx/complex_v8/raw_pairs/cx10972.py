from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("voltage") < 233.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw").between(96.9, 446.9))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

qualified = (
    combined_details_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 231.7), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    qualified.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    with_stats
    .cube("site_id", "quality", "site_zone")
    .agg(
        F.round(F.sum("voltage") / F.nullif(F.sum("site_installed_kw"), F.lit(0)), 3).alias("voltage_to_site_installed_kw_ratio"),
        F.min("kwh").alias("min_kwh"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
    )
)
