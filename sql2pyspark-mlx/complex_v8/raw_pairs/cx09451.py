from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")) | (F.col("voltage") >= 216.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.sum("voltage").alias("total_reading_voltage"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
    )
)

with_counts = (
    recent_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        "m.total_reading_voltage",
        "m.avg_reading_kwh",
    )
)

with_details = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.year(F.col("si.commissioned_on")) == 2024)
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.kwh_minus_voltage",
        "s.voltage_per_kwh",
        "s.first_reading_read_at",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_parent_site = (
    with_details.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        "si.site_name",
    )
)

exploded_detail = (
    with_parent_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.voltage_per_kwh",
        "s.site_name",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["reading_quality", "site_id"], ["reading_quality"], []],
        "reading_quality", "site_id",
    )
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("site_name"))).like("D%"), F.col("reading_voltage")).otherwise(0)).alias("reading_voltage_upper_trim_site_name_like_d"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
    .select(
        F.coalesce(F.col("reading_quality"), F.lit("ALL")).alias("reading_quality"),
        "site_id",
        "reading_voltage_upper_trim_site_name_like_d",
        "distinct_site_ids",
    )
)
