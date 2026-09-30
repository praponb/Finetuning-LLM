from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 237.5) | (F.col("kwh") > 28.4))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_details = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") < 284.0)
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_date",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

enriched = (
    with_details
    .withColumn("days_since_read_at_date", F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at_date")))
)

quality_stats = (
    enriched
    .groupBy("quality")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

outliers = (
    enriched.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("kwh").alias("total_reading_kwh"))
)

summarized_rows = (
    outliers.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.total_reading_kwh")
)

annotated = (
    summarized_rows
    .select(
        "*",
        (F.floor(F.col("total_reading_kwh") / 100) * 100).alias("total_reading_kwh_bucket"),
        F.substring(F.trim(F.col("site_zone")), 1, 4).alias("site_zone_prefix"),
    )
)

result = (
    annotated
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "site_installed_kw",
        "site_zone",
        "site_commissioned_on",
        "days_since_read_at_date",
        "kwh_minus_avg",
        "total_reading_kwh",
        "total_reading_kwh_bucket",
    )
    .orderBy(
        F.col("kwh").desc_nulls_first(),
        F.col("total_reading_kwh_bucket").desc_nulls_first(),
    )
)
