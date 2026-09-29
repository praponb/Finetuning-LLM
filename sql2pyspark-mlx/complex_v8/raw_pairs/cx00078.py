from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("voltage") <= 233.1)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.abs(F.col("voltage") - 227.6).alias("voltage_distance"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

summarized_rows = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "voltage_distance",
        "voltage",
        "kwh",
    )
    .unpivot(
        ids=["id"],
        values=["voltage_distance", "voltage", "kwh"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "measure")
)
