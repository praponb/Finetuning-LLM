from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

screened = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 80.8), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_counts = (
    screened.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_activity = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") < 386.6) & (F.col("si.commissioned_on") >= F.lit("2024-06-27").cast("date")), "left_anti")
)

with_parent_site = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on").isNotNull()), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_year",
        "s.voltage_filled",
        "s.reading_count",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent_site
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "read_at_year",
        "reading_count",
        "site_name",
        "site_installed_kw",
        F.abs(F.col("reading_count") - 4).alias("reading_count_distance"),
        F.when(F.col("voltage_filled") >= 234.5, "HIGH").when(F.col("voltage_filled") >= 218.8, "MEDIUM").otherwise("LOW").alias("voltage_filled_tier"),
    )
    .orderBy("quality", "reading_count_distance")
)
