from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc(), F.col("id").desc())

base = (
    df_meter_readings
    .filter(F.col("site_id") <= 4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_parent_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

latest_per_key = (
    with_parent_site
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest_per_key.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

enriched = (
    joined
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.dayofweek(F.col("site_commissioned_on")).alias("site_commissioned_on_dayofweek"),
        (F.floor(F.col("site_installed_kw") / 100) * 100).alias("site_installed_kw_bucket"),
    )
)

result = (
    enriched
    .filter(F.upper(F.trim(F.col("site_name"))).like("D%"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "site_zone",
        "site_name",
        "site_commissioned_on",
        "voltage_per_kwh",
        "site_commissioned_on_dayofweek",
        "site_installed_kw_bucket",
        F.initcap(F.trim(F.col("site_tariff"))).alias("site_tariff_clean"),
    )
)
