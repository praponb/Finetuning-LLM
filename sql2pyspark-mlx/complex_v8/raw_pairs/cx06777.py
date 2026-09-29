from pyspark.sql import functions as F

filtered_sites = (
    df_sites
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_add(F.col("commissioned_on"), 30).alias("commissioned_on_plus_30d"),
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    filtered_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

tagged = (
    summarized_rows
    .filter(F.col("installed_kw") <= 284.0)
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "commissioned_on_plus_30d",
        "reading_count",
        "first_reading_read_at",
        F.concat_ws("-", F.col("tariff"), F.col("zone")).alias("tariff_zone_label"),
    )
)

result = (
    tagged
    .groupBy("reading_count")
    .pivot("tariff", ["FLAT", "TOU", "EV"])
    .agg(F.sum("installed_kw"))
    .orderBy("reading_count")
)
