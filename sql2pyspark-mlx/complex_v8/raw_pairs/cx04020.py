from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("read_at_dayofweek").desc(), F.col("id").desc())

scoped = (
    df_meter_readings
    .filter(F.col("voltage") >= 219.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-05-10").cast("date")))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.read_at_dayofweek",
        "s.read_at_month",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.read_at_dayofweek",
        "s.read_at_month",
        "s.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
        F.col("si.site_name").alias("site_site_name"),
    )
)

enriched_site = (
    enriched_2.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.read_at_dayofweek",
        "s.site_name",
        "s.site_site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.site_name").alias("site_site_name_2"),
    )
)

latest_per_key = (
    enriched_site
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "site_name",
        "site_installed_kw",
        "site_site_name_2",
    )
    .orderBy("read_at", F.col("id").desc())
    .limit(5)
)
