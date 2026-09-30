from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .filter(F.col("voltage") <= 240.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

latest_per_key = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded_detail = (
    latest_per_key.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

with_parent = (
    exploded_detail.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .filter(~F.col("m.quality").isin("EST", "OK"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.reading_id",
        "s.reading_kwh",
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    with_parent
    .groupBy("reading_id")
    .pivot("reading_quality_2", ["OK", "EST"])
    .agg(F.max("reading_kwh"))
    .orderBy("reading_id")
)
