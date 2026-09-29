from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone").orderBy(F.col("installed_kw").desc(), F.col("id").desc())

valid = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2025-05-07").cast("date")) & (F.col("tariff").isin("FLAT", "TOU")))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("quarter", F.col("commissioned_on")).alias("commissioned_on_quarter"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

summarized_rows = (
    deduped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

with_children_readings = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.commissioned_on",
        "s.commissioned_on_quarter",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    with_children_readings
    .filter(F.month(F.col("commissioned_on_quarter")).isin(1, 2))
    .select(
        "id",
        "reading_count",
        "reading_id",
        "reading_read_at",
        "reading_kwh",
        "reading_quality",
        "reading_voltage",
    )
)
