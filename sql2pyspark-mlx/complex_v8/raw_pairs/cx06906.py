from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

qualified = (
    cleaned_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 235.0), "left_anti")
    .filter(F.col("quality").isNotNull())
)

without_matches = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(~F.col("quality").isin("BAD", "OK"), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    without_matches.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .select("id", "reading_count", "read_at_month")
    .unpivot(
        ids=["id"],
        values=["reading_count", "read_at_month"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
