from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), F.col("id").desc(), F.col("reading_id").desc())

scoped = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

detail_rows = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 216.1), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        "s.kwh_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

unique_rows = (
    detail_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_details_reading = (
    unique_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (F.col("m.voltage").between(221.4, 238.8)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_label",
        "s.kwh_adj",
        "s.reading_id",
        "s.reading_voltage",
        F.col("m.voltage").alias("reading_voltage_2"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    with_details_reading
    .select(
        "id",
        "site_id",
        "quality",
        "read_at_label",
        "kwh_adj",
        "reading_id",
        "reading_voltage",
        "reading_voltage_2",
        "reading_quality_2",
        (F.col("read_at") + F.expr("INTERVAL 15 MINUTES")).alias("read_at_plus_15m"),
    )
    .orderBy(F.col("quality").asc(), F.col("reading_voltage_2").asc())
)
