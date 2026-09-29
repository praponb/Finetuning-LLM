from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent_site = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(~F.col("si.tariff").isin("FLAT", "EV"))
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

scored = (
    with_parent_site
    .filter(F.col("site_tariff") != "TOU")
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "site_installed_kw",
        F.round(F.col("site_installed_kw") * 0.25, 2).alias("site_installed_kw_adj"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_parent_site_2 = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.site_installed_kw",
        "s.site_installed_kw_adj",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent_site_2
    .groupBy("quality", "site_commissioned_on")
    .pivot("site_zone", ["Z2", "Z3"])
    .agg(F.count("kwh").alias("count_v"), F.sum("kwh").alias("sum_v"))
)
