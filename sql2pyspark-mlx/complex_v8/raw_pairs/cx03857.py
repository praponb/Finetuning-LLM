from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter(F.col("kwh") > 16.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("kwh").alias("total_reading_kwh"),
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_kwh",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

exploded_detail_readings = (
    with_counts.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 237.5), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.total_reading_kwh",
        "s.last_reading_read_at",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    exploded_detail_readings
    .select(
        "id",
        "reading_id",
        "reading_voltage",
        "total_reading_kwh",
        "reading_kwh",
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["reading_voltage", "total_reading_kwh", "reading_kwh"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
