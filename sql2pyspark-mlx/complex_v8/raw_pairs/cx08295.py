from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("site_id") >= 5) | (F.col("kwh") >= 29.2))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

per_group = (
    cleaned
    .groupBy("quality", "site_id")
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.sum("voltage").alias("total_voltage"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("quality") != "BAD", F.col("kwh")).otherwise(0)).alias("kwh_quality_bad"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_to_voltage_ratio"),
    )
    .filter(F.col("row_count") >= 3)
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "EST", 1)).alias("reading_quality_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at_2"),
    )
)

with_counts = (
    per_group.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.last_reading_read_at",
        "m.last_reading_read_at_2",
    )
)

derived = (
    with_counts
    .select(
        "quality",
        "site_id",
        "distinct_qualitys",
        "total_voltage",
        "row_count",
        "kwh_quality_bad",
        "quality_list",
        "kwh_to_voltage_ratio",
        "reading_quality_count",
        "last_reading_read_at",
        "last_reading_read_at_2",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.date_format(F.col("last_reading_read_at"), "yyyy-MM").alias("last_reading_read_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

result = (
    derived
    .select(
        "quality",
        "site_id",
        "distinct_qualitys",
        "row_count",
        "kwh_quality_bad",
        "quality_list",
        "kwh_to_voltage_ratio",
        "last_reading_read_at_2",
        "quality_prefix",
        "last_reading_read_at_label",
        "quality_slug",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_reading_read_at")).alias("days_since_last_reading_read_at"),
    )
    .orderBy(F.col("last_reading_read_at_2").desc())
)
