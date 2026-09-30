from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy("voltage", "id")
w_read_at_month_site_tariff = Window.partitionBy(F.date_trunc("month", "read_at"), "site_tariff")

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "OK"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

combined_details = (
    valid_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff").isin("FLAT", "TOU"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_group",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.lead("voltage").over(w_all_by_voltage).alias("next_voltage"),
        F.count(F.lit(1)).over(w_read_at_month_site_tariff).alias("rows_in_read_at_month_site_tariff"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_month_site_tariff), 2).alias("voltage_vs_read_at_month_site_tariff_avg"),
    )
)

with_details = (
    metrics.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_group",
        "s.site_tariff",
        "s.site_commissioned_on",
        "s.rows_in_read_at_month_site_tariff",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

result = (
    with_details
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_group",
        "site_commissioned_on",
        "site_tariff_2",
    )
    .orderBy("site_tariff_2", "id")
    .limit(10)
)
