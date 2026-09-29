from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(10, 15))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

enriched = (
    scoped
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
        F.count(F.when(F.col("quality").isin("EST", "BAD"), 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

derived = (
    with_stats
    .filter(F.col("first_reading_read_at") >= F.lit("2025-01-09 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.abs(F.col("voltage") - 227.3).alias("voltage_distance"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

result = (
    derived
    .groupingSets(
        [["reading_quality_count", "quality_or_unknown", "quality"], ["reading_quality_count"], []],
        "reading_quality_count", "quality_or_unknown", "quality",
    )
    .agg(
        F.min("voltage_distance").alias("min_voltage_distance"),
        F.round(F.avg("reading_quality_count"), 2).alias("avg_reading_quality_count"),
        F.round(F.avg("voltage"), 2).alias("avg_voltage"),
    )
    .orderBy(
        F.col("reading_quality_count").asc_nulls_last(),
        F.col("quality_or_unknown").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
