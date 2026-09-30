from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year = Window.partitionBy(F.year("read_at"))

base = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-02 13:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

enriched_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage_minus_kwh",
        "s.quality_ref",
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_windows = (
    enriched_site
    .withColumn("voltage_minus_kwh_share_of_read_at_year", F.round(F.col("voltage_minus_kwh") / F.nullif(F.sum("voltage_minus_kwh").over(w_read_at_year), F.lit(0)), 4))
    .withColumn("voltage_minus_kwh_vs_read_at_year_avg", F.round(F.col("voltage_minus_kwh") - F.avg("voltage_minus_kwh").over(w_read_at_year), 2))
)

detail_rows = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality_ref",
        "s.site_tariff",
        "s.voltage_minus_kwh_vs_read_at_year_avg",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    detail_rows
    .filter(F.col("reading_kwh").between(30.2, 85.7))
    .select(
        "id",
        "quality_ref",
        "site_tariff",
        "voltage_minus_kwh_vs_read_at_year_avg",
        "reading_id",
        "reading_quality",
        "reading_kwh",
        "reading_read_at",
        "reading_voltage",
    )
)
