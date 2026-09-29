from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")
w_quality_by_voltage_full = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

metrics = (
    valid_readings
    .select(
        "*",
        F.round(F.avg("voltage_per_kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_voltage_per_kwh"),
        F.round(F.col("voltage_per_kwh") - F.avg("voltage_per_kwh").over(w_site_id_quality), 2).alias("voltage_per_kwh_vs_site_id_quality_avg"),
        F.lead("read_at", 3).over(w_site_id_by_voltage).alias("next3_read_at"),
        F.last("voltage_plus_kwh").over(w_quality_by_voltage_full).alias("last_voltage_plus_kwh"),
    )
)

latest_per_key = (
    metrics
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    latest_per_key.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "BAD"), "left_anti")
)

with_details = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.voltage_plus_kwh",
        "s.site_id_quality_avg_voltage_per_kwh",
        "s.voltage_per_kwh_vs_site_id_quality_avg",
        "s.last_voltage_plus_kwh",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_parent_site = (
    with_details.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.voltage_plus_kwh",
        "s.site_id_quality_avg_voltage_per_kwh",
        "s.voltage_per_kwh_vs_site_id_quality_avg",
        "s.site_installed_kw",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
    )
)

screened = (
    with_parent_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)) & (F.col("m.voltage") >= 222.6), "left_anti")
)

result = (
    screened
    .filter(F.col("site_installed_kw") < 284.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "site_id_quality_avg_voltage_per_kwh",
        "site_installed_kw",
        "site_tariff",
        "site_commissioned_on_2",
    )
    .orderBy(F.col("site_installed_kw").asc_nulls_last(), "site_commissioned_on_2")
)
