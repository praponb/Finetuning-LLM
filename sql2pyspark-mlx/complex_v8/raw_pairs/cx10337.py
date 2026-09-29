from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("voltage") <= 235.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 232.0, "A").when(F.col("voltage") >= 221.8, "B").otherwise("C").alias("voltage_tier"),
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

combined_details = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.read_at_month",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
    )
)

filtered = (
    combined_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 221.4) & (F.col("m.quality") == "OK"), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "site_id",
        "quality",
        "read_at_month",
        "site_installed_kw",
        "site_name",
        "site_tariff",
        F.date_format(F.col("read_at_month"), "MMM yyyy").alias("read_at_month_label"),
    )
)
