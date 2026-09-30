from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 232.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.min("kwh").alias("min_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    base_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.min_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

summary = (
    profiled
    .groupBy(
        "quality_or_unknown",
        "reading_count",
        F.date_format("last_reading_read_at", "yyyy-MM").alias("last_reading_read_at_ym"),
    )
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.count("*").alias("n_rows"),
    )
)

enriched = (
    summary
    .select(
        "*",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.length(F.trim(F.col("last_reading_read_at_ym"))).alias("last_reading_read_at_ym_len"),
    )
)

result = (
    enriched
    .select(
        "quality_or_unknown",
        "reading_count",
        "last_reading_read_at_ym",
        "distinct_qualitys",
        "n_rows",
        "n_rows_bucket",
        "last_reading_read_at_ym_len",
    )
    .orderBy(F.col("reading_count").asc_nulls_last())
)
