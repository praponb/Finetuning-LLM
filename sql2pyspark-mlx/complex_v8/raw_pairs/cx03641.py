from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("site_id") <= 7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

joined_site = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.length(F.trim(F.col("si.site_name"))) >= 5), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_details = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.site_tariff",
        "s.site_name",
        "s.site_commissioned_on",
        F.col("si.tariff").alias("site_tariff_2"),
        F.col("si.site_name").alias("site_site_name"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details
    .groupBy("read_at")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.count("site_installed_kw").alias("count_v"), F.max("site_installed_kw").alias("max_v"))
    .orderBy("read_at")
)
