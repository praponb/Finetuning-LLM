from pyspark.sql import functions as F

scoped = (
    df_sites
    .filter(F.col("zone") == "Z3")
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.quarter(F.col("commissioned_on")).alias("commissioned_on_quarter"),
        F.when(F.col("installed_kw") >= 373.3, "A").when(F.col("installed_kw") >= 96.9, "B").otherwise("C").alias("installed_kw_tier"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.first_reading_read_at",
    )
)

result = (
    with_counts
    .groupBy("commissioned_on_quarter")
    .pivot("zone", ["Z3", "Z2", "Z4"])
    .agg(F.max("reading_quality_count").alias("max_v"), F.sum("reading_quality_count").alias("sum_v"))
    .orderBy("commissioned_on_quarter")
)
