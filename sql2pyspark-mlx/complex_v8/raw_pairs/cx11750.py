from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), F.col("id").desc())

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 43.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
)

latest_per_key = (
    valid_readings
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_stats = (
    latest_per_key.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        "m.first_reading_read_at",
        "m.last_reading_read_at",
    )
)

joined_site = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.voltage_bucket",
        "s.total_reading_voltage",
        "s.first_reading_read_at",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "first_reading_read_at",
    )
    .orderBy(
        F.col("voltage").desc(),
        F.col("first_reading_read_at").desc_nulls_first(),
    )
)
