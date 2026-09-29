from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("voltage") < 238.3) | (F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.abs(F.col("kwh") - 78.6).alias("kwh_distance"),
    )
)

site_id_stats = (
    recent
    .groupBy("site_id")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

vs_group = (
    recent.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

with_activity = (
    vs_group.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")) & (F.col("m.voltage") < 234.5), "left_anti")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count(F.when(~F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

summarized_rows = (
    with_activity.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "avg_kwh",
        "kwh_distance",
        "kwh_minus_avg",
        "kwh",
    )
    .unpivot(
        ids=["id"],
        values=["avg_kwh", "kwh_distance", "kwh_minus_avg", "kwh"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
