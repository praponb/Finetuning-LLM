from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-02-24 14:00:00").cast("timestamp")) | (F.col("quality").isin("EST", "OK")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 86.0, "A").when(F.col("kwh") >= 52.4, "B").otherwise("C").alias("kwh_tier"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_parent_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

filtered_2 = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2025-05-10").cast("date")), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

with_counts = (
    filtered_2.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    with_counts
    .filter(F.col("kwh") < 78.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "kwh_tier",
        "site_installed_kw",
        "site_tariff",
        "site_zone",
        "first_reading_read_at",
        F.lower(F.regexp_replace(F.trim(F.col("site_tariff")), " ", "_")).alias("site_tariff_slug"),
        F.abs(F.col("kwh") - 74.3).alias("kwh_distance"),
    )
    .orderBy("kwh", F.col("kwh_distance").desc(), "id")
    .limit(10)
)
