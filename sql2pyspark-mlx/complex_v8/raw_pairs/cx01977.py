from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_hour = Window.partitionBy("read_at_hour")
w_quality_read_at_month_by_vol = Window.partitionBy("quality", F.month("read_at")).orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_read_at_hour_by_volt = Window.partitionBy("quality", "read_at_hour").orderBy(F.col("voltage").desc(), "id")

scoped_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") <= 81.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

windowed = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_slug",
        "read_at_hour",
        "quality_clean",
        F.round(F.col("read_at_hour") - F.avg("read_at_hour").over(w_read_at_hour), 2).alias("read_at_hour_vs_read_at_hour_avg"),
        F.last("read_at_hour").over(w_quality_read_at_month_by_vol).alias("last_read_at_hour"),
        F.rank().over(w_quality_read_at_hour_by_volt).alias("voltage_rank"),
    )
)

with_details_site = (
    windowed.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

filtered = (
    with_details_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("m.voltage") > 222.1), "left_semi")
)

screened = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()) & (F.col("m.voltage") > 214.3), "left_semi")
    .filter(F.col("read_at_hour") <= 14)
)

result = (
    screened
    .filter(F.col("last_read_at_hour") >= 8)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "quality_slug",
        "read_at_hour",
        "quality_clean",
        "read_at_hour_vs_read_at_hour_avg",
        "voltage_rank",
        "site_installed_kw",
    )
)
