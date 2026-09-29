from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_by_last_reading = Window.partitionBy("site_tariff").orderBy(F.col("last_reading_read_at").desc(), "id")
w_site_id_by_last_reading_read = Window.partitionBy("site_id").orderBy("last_reading_read_at", "id")

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

profiled = (
    recent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

with_activity = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 96.8), "left_semi")
    .filter(F.col("quality") == "OK")
)

with_parent = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.tariff") == "EV")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.last_reading_read_at",
        F.col("si.tariff").alias("site_tariff"),
    )
)

combined_details_site = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

qualified = (
    combined_details_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "OK") & (F.col("m.read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")), "left_semi")
)

ranked = (
    qualified
    .select(
        "*",
        (F.col("kwh") - F.lag("kwh").over(w_site_tariff_by_last_reading)).alias("kwh_change"),
        F.round(F.cume_dist().over(w_site_id_by_last_reading_read), 3).alias("last_reading_read_at_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_name",
        "site_tariff_2",
        "kwh_change",
        F.substring(F.trim(F.col("site_tariff_2")), 1, 2).alias("site_tariff_2_prefix"),
    )
    .orderBy(F.col("kwh_change").asc_nulls_last(), "site_name")
)
