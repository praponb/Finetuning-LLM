from pyspark.sql import functions as F

valid_readings = df_meter_readings.filter((F.col("voltage") >= 223.3) & (F.col("quality") == "OK"))

totals = (
    valid_readings
    .groupBy(
        "site_id",
        "quality",
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
    )
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
        F.min("kwh").alias("min_kwh"),
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
    )
)

profiled = (
    totals.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    profiled
    .filter(F.col("min_kwh") > 12.0)
    .select(
        "site_id",
        "quality",
        "read_at_ym",
        "avg_kwh",
        "min_kwh",
        "reading_quality_count",
        F.greatest(F.col("reading_quality_count"), F.col("reading_count")).alias("greatest_reading_quality_count_reading_count"),
        F.length(F.trim(F.col("read_at_ym"))).alias("read_at_ym_len"),
    )
    .orderBy(
        F.col("min_kwh").desc(),
        F.col("reading_quality_count").desc(),
        "site_id",
        "quality",
        "read_at_ym",
    )
    .limit(5)
)
