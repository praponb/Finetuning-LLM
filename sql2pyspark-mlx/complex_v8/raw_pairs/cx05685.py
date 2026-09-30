from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("reading_voltage_2").desc(), F.col("id").desc(), F.col("reading_id").desc(), F.col("reading_id_2").desc())
w_latest_2 = Window.partitionBy("site_id").orderBy("reading_count_2", "id", "reading_id", "reading_id_2")

scoped = (
    df_meter_readings
    .filter((F.col("kwh") < 86.0) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("voltage") >= 238.8, "large").when(F.col("voltage") >= 220.2, "medium").otherwise("small").alias("voltage_tier"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

with_children_readings = (
    with_activity.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

with_children_readings_2 = (
    with_children_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at_label",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_kwh",
        "s.reading_read_at",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.voltage").alias("reading_voltage_2"),
    )
)

latest = (
    with_children_readings_2
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("kwh").alias("max_reading_kwh"),
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_counts = (
    latest.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.max_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

latest_per_key = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "site_id",
        "read_at_label",
        "reading_id",
        "reading_read_at",
        "reading_id_2",
        "reading_read_at_2",
        "max_reading_kwh",
        "reading_count_2",
        F.when(F.col("reading_count_2") >= 6, "large").when(F.col("reading_count_2") >= 2, "medium").otherwise("small").alias("reading_count_2_tier"),
    )
    .orderBy(F.col("reading_id_2").desc_nulls_first(), "read_at_label")
)
