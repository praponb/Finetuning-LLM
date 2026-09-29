from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

site_id_stats = (
    recent_readings
    .groupBy("site_id")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

compared = (
    recent_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
    )
)

with_counts = (
    compared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

grouped = (
    with_counts
    .groupBy("site_id", "quality")
    .agg(
        F.round(F.avg("voltage_minus_avg"), 2).alias("avg_voltage_minus_avg"),
        F.min("read_at").alias("first_read_at"),
    )
)

labeled = (
    grouped
    .select(
        "site_id",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

result = (
    labeled
    .select(
        "site_id",
        "quality",
        "quality_len",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug_2"),
    )
)
