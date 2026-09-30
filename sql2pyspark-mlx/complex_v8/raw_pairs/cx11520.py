from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("site_id") >= 9)
    .select(
        "id",
        "site_id",
        "read_at",
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
    )
)

summarized_rows = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("kwh").alias("max_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
        F.min("voltage").alias("min_reading_voltage"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_kwh",
        "m.last_reading_read_at",
        "m.min_reading_voltage",
    )
)

result = (
    with_counts
    .cube("reading_quality_count", "quality", "site_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("min_reading_voltage"), 2).alias("stddev_min_reading_voltage"),
    )
    .orderBy(
        F.col("reading_quality_count").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
