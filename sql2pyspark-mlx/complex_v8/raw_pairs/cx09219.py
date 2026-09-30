from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_site_id = Window.partitionBy("site_tariff", "site_id")
w_quality_by_min_reading_volta = Window.partitionBy("quality").orderBy(F.col("min_reading_voltage").asc_nulls_last(), "id")

scoped = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("voltage").between(219.6, 238.3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.min_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

qualified = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("quality") != "OK")
)

combined_details = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.voltage_plus_kwh",
        "s.min_reading_voltage",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

screened = (
    combined_details.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 35.9) & (F.col("m.quality").isNotNull()), "left_anti")
)

ranked = (
    screened
    .select(
        "id",
        "site_id",
        "min_reading_voltage",
        "site_name",
        F.sum("voltage").over(w_site_tariff_site_id).alias("site_tariff_site_id_sum_voltage"),
        F.round(F.col("min_reading_voltage") - F.avg("min_reading_voltage").over(w_site_tariff_site_id), 2).alias("min_reading_voltage_vs_site_tariff_site_id_avg"),
        F.row_number().over(w_quality_by_min_reading_volta).alias("rn"),
        F.round(F.cume_dist().over(w_quality_by_min_reading_volta), 3).alias("min_reading_voltage_cume_dist"),
    )
)

combined_details_site = (
    ranked.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    combined_details_site
    .select(
        "id",
        "site_tariff_site_id_sum_voltage",
        "min_reading_voltage_vs_site_tariff_site_id_avg",
        "min_reading_voltage_cume_dist",
        "site_installed_kw",
        "site_tariff",
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
    .orderBy(
        F.col("min_reading_voltage_vs_site_tariff_site_id_avg").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
