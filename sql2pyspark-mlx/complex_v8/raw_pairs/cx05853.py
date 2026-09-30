from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

annotated = (
    filtered_readings
    .filter(F.col("site_id") >= 3)
    .withColumn("days_since_read_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at"),
        F.max("kwh").alias("max_reading_kwh"),
    )
)

summarized_rows = (
    annotated.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at",
        "m.max_reading_kwh",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "read_at_hour",
        "kwh_per_voltage",
        "days_since_read_at",
        "reading_count",
        "last_reading_read_at",
    )
    .orderBy(F.col("last_reading_read_at").desc(), F.col("quality").asc_nulls_last())
)
