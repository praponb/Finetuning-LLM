from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality", "site_id").orderBy(F.col("reading_kwh").desc(), F.col("id").desc(), F.col("reading_id").desc())

cleaned = (
    df_meter_readings
    .filter(F.col("voltage") >= 216.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

detail_rows_readings = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

enriched = (
    detail_rows_readings
    .select(
        "*",
        F.round(F.col("reading_voltage") * 0.25, 2).alias("reading_voltage_adj"),
        F.round(F.col("voltage") * 1.5, 2).alias("voltage_adj"),
    )
)

latest_per_key = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

site_id_stats = (
    latest_per_key
    .groupBy("site_id")
    .agg(F.avg("reading_voltage").alias("avg_reading_voltage"))
)

vs_group = (
    latest_per_key.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.reading_voltage") > F.col("g.avg_reading_voltage"))
    .select(
        "s.*",
        "g.avg_reading_voltage",
        F.round(F.col("s.reading_voltage") - F.col("g.avg_reading_voltage"), 2).alias("reading_voltage_minus_avg"),
    )
)

with_details_site = (
    vs_group.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_kwh",
        "s.reading_read_at",
        "s.reading_voltage_adj",
        "s.voltage_adj",
        "s.avg_reading_voltage",
        "s.reading_voltage_minus_avg",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details_site
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "read_at_label",
        "reading_id",
        "reading_kwh",
        "reading_read_at",
        "reading_voltage_adj",
        "voltage_adj",
        "reading_voltage_minus_avg",
        "site_installed_kw",
        F.coalesce(F.col("reading_voltage_adj"), F.lit(0.0)).alias("reading_voltage_adj_filled"),
        F.concat(F.col("read_at_label"), F.lit("#"), F.col("reading_id").cast("string")).alias("read_at_label_ref"),
    )
    .orderBy(F.col("site_id").desc(), F.col("reading_read_at").asc())
)
