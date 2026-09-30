from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality_or_unknown", "site_id").orderBy(F.col("voltage_adj").desc(), F.col("id").desc())

base = (
    df_meter_readings
    .filter(F.col("quality") != "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

unique_rows = (
    base
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

exploded_detail_readings = (
    unique_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.voltage_adj",
        "s.quality_or_unknown",
        "s.quality_prefix",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

exploded_detail = (
    exploded_detail_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage_adj",
        "s.quality_or_unknown",
        "s.quality_prefix",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_kwh",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    exploded_detail
    .groupBy("reading_id_2", "reading_read_at_2")
    .pivot("reading_quality", ["BAD", "OK"])
    .agg(F.sum("voltage_adj"))
    .orderBy("reading_id_2", "reading_read_at_2")
)
