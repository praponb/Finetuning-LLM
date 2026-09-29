from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("kwh") >= 51.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("kwh").alias("max_reading_kwh"),
        F.count("*").alias("reading_count_2"),
    )
)

profiled = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.max_reading_kwh",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

enriched = (
    profiled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", "si.site_name")
)

enriched_site = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.commissioned_on").alias("site_commissioned_on"))
)

result = (
    enriched_site
    .rollup("reading_count_2", "reading_count", "site_id")
    .agg(
        F.round(F.sum("voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("voltage_to_kwh_ratio"),
    )
    .orderBy(
        F.col("reading_count_2").asc_nulls_last(),
        F.col("reading_count").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
