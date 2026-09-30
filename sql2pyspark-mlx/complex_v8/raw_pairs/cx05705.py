from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id", "quality").orderBy(F.col("voltage").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("quality", "site_id").orderBy("kwh", "id")

prepared_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
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
    prepared_readings
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("voltage").alias("min_reading_voltage"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

profiled = (
    latest_per_key.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.min_reading_voltage",
        "m.first_reading_read_at",
    )
)

latest = (
    profiled
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_activity = (
    latest.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

unique_rows = (
    with_activity
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined = (
    unique_rows.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date")), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    joined
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "reading_count",
        "min_reading_voltage",
        "site_name",
        "site_commissioned_on",
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
    )
    .orderBy(F.col("read_at").desc_nulls_first(), "id")
    .limit(20)
)
