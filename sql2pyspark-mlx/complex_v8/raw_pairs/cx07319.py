from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("kwh") >= 20.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.min("voltage").alias("min_reading_voltage"),
    )
)

profiled = (
    scoped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
        "m.min_reading_voltage",
    )
)

tagged = (
    profiled
    .select(
        "id",
        "site_id",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["quality", "quality_or_unknown", "reading_count"], ["quality"], []],
        "quality", "quality_or_unknown", "reading_count",
    )
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.round(F.stddev("reading_count_2"), 2).alias("stddev_reading_count_2"),
        F.round(F.avg("reading_count_2"), 2).alias("avg_reading_count_2"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("quality_or_unknown"), F.lit("ALL")).alias("quality_or_unknown"),
        "reading_count",
        "is_total_quality",
        "stddev_reading_count_2",
        "avg_reading_count_2",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("quality_or_unknown").asc_nulls_last(),
        F.col("reading_count").asc_nulls_last(),
    )
)
