from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("quality") == "OK") | (F.col("kwh") < 83.8))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count_2"),
    )
)

profiled = (
    recent_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count_2"), F.lit(0)).alias("reading_quality_count_2"),
    )
)

enriched = (
    profiled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.reading_quality_count",
        "s.reading_count",
        "s.reading_quality_count_2",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
        F.count("*").alias("reading_count_2"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.last_reading_read_at_2",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    with_counts
    .select("id", "reading_quality_count", "reading_count")
    .unpivot(
        ids=["id"],
        values=["reading_quality_count", "reading_count"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
)
