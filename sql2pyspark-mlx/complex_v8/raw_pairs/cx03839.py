from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

combined_details = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_prefix",
        "s.voltage_minus_kwh",
        "s.read_at_week",
        F.col("si.zone").alias("site_zone"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    combined_details.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.min_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["quality", "site_zone"], ["quality"], []],
        "quality", "site_zone",
    )
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.round(F.sum("kwh"), 2).alias("total_kwh"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        "is_total_quality",
        "total_kwh",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_zone").asc_nulls_last())
)
