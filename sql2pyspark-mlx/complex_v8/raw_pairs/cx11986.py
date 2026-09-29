from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_id_by_reading_count = Window.partitionBy("reading_id").orderBy(F.col("reading_count").asc_nulls_last(), "id", "reading_id")
w_reading_quality = Window.partitionBy("reading_quality")

cleaned_readings = (
    df_meter_readings
    .filter((F.col("quality") == "EST") & (F.col("read_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")))
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
    .agg(F.count("*").alias("reading_count"))
)

summarized_rows = (
    cleaned_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

line_level = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_trends = (
    line_level
    .select(
        "*",
        F.row_number().over(w_reading_id_by_reading_count).alias("seq_no"),
        F.size(F.collect_set("reading_id").over(w_reading_quality)).alias("distinct_reading_id_in_reading_quality"),
    )
)

leaders = (
    with_trends
    .filter((F.col("seq_no") == 1) & (F.hour(F.col("reading_read_at")).between(10, 12)))
    .drop("seq_no")
)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "reading_count",
        "reading_id",
        "reading_voltage",
        "reading_kwh",
        "reading_quality",
        F.lower(F.trim(F.col("reading_quality"))).alias("reading_quality_clean"),
        F.round((F.unix_timestamp(F.col("reading_read_at")) - F.unix_timestamp(F.col("read_at"))) / 3600, 1).alias("hours_read_at_to_reading_read_at"),
    )
    .orderBy(F.col("hours_read_at_to_reading_read_at").desc())
)
