from pyspark.sql import functions as F

recent = (
    df_meter_readings
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
        F.max("voltage").alias("max_reading_voltage"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    recent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_voltage",
        "m.avg_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

labeled = (
    profiled
    .filter(F.col("max_reading_voltage") >= 224.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "max_reading_voltage",
        "avg_reading_kwh",
        "reading_count",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

tagged = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "avg_reading_kwh",
        "reading_count",
        "days_since_read_at",
        "quality_slug",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

quality_stats = (
    tagged
    .groupBy("quality")
    .agg(F.max("quality_len").alias("max_quality_len"))
)

outliers = (
    tagged.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.quality_len") == F.col("g.max_quality_len"))
    .select(
        "s.*",
        "g.max_quality_len",
        F.round(F.col("s.quality_len") - F.col("g.max_quality_len"), 2).alias("quality_len_minus_max"),
    )
)

site_id_stats = (
    outliers
    .groupBy("site_id")
    .agg(F.avg("days_since_read_at").alias("avg_days_since_read_at"))
)

compared = (
    outliers.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.days_since_read_at") > F.col("g.avg_days_since_read_at"))
    .select(
        "s.*",
        "g.avg_days_since_read_at",
        F.round(F.col("s.days_since_read_at") - F.col("g.avg_days_since_read_at"), 2).alias("days_since_read_at_minus_avg"),
    )
)

rollup_base = (
    compared
    .groupBy("reading_count", "quality")
    .agg(F.min("read_at").alias("first_read_at"))
)

result = (
    rollup_base
    .select(
        "reading_count",
        "quality",
        "first_read_at",
        F.coalesce(F.col("reading_count"), F.lit(0)).alias("reading_count_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
    .orderBy("quality_slug")
)
