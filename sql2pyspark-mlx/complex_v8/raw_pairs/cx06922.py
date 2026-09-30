from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(7, 13)), "left_anti")
)

result = (
    with_activity
    .filter(F.col("site_installed_kw") <= 373.3)
    .select(
        "id",
        "site_id",
        "quality",
        "site_commissioned_on",
    )
    .orderBy(F.col("site_id").desc(), "id")
    .limit(5)
)
