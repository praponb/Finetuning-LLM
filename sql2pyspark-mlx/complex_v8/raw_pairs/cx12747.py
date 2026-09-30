from pyspark.sql import functions as F

base = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
    )
)

with_details_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

summary = (
    with_details_site
    .groupBy(
        "quality",
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
        "site_tariff",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("site_commissioned_on").alias("first_site_commissioned_on"),
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
    )
)

result = (
    summary
    .select(
        "quality",
        "site_tariff",
        "cnt",
        "first_site_commissioned_on",
        "site_tariff_list",
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
    .orderBy(F.col("quality").asc())
)
