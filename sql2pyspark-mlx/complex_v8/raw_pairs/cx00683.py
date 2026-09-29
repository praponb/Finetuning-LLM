from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())

cleaned_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-02-06 14:00:00").cast("timestamp")) & (F.col("kwh") <= 83.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("voltage").alias("min_reading_voltage"))
)

profiled = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.min_reading_voltage")
)

deduped = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_parent_site = (
    deduped.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_week",
        "s.min_reading_voltage",
        F.col("si.tariff").alias("site_tariff"),
    )
)

annotated = (
    with_parent_site
    .select(
        "*",
        F.date_format(F.col("read_at_week"), "yyyy-MM-dd HH:00").alias("read_at_week_label"),
        F.lower(F.trim(F.col("site_tariff"))).alias("site_tariff_clean"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    annotated.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_id",
        "read_at",
        "read_at_week",
        "min_reading_voltage",
        "site_tariff_clean",
        "reading_count",
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("site_id").cast("string")).alias("site_tariff_ref"),
        (F.col("reading_count") - F.col("min_reading_voltage")).alias("reading_count_minus_min_reading_voltage"),
    )
    .orderBy("reading_count", F.col("site_id").desc_nulls_first())
)
