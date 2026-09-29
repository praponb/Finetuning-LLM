from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality", "site_id").orderBy(F.col("voltage").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "BAD")) & (F.col("site_id").isNotNull()))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

grouped = (
    latest
    .filter(F.col("kwh") > 48.2)
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "site_id")
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("quality") == "BAD", F.col("voltage"))).alias("voltage_quality_bad"),
        F.countDistinct("quality").alias("distinct_qualitys"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
        F.max("kwh").alias("max_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

with_counts = (
    grouped.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.max_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

with_activity = (
    with_counts
    .crossJoin(with_counts.agg(F.max("voltage_quality_bad").alias("overall_max_voltage_quality_bad")))
    .filter(F.col("voltage_quality_bad") < F.col("overall_max_voltage_quality_bad"))
    .drop("overall_max_voltage_quality_bad")
)

with_children_readings = (
    with_activity.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.read_at_month",
        "s.site_id",
        "s.cnt",
        "s.voltage_quality_bad",
        "s.distinct_qualitys",
        "s.max_reading_kwh",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    with_children_readings
    .select(
        "read_at_month",
        "site_id",
        "cnt",
        "voltage_quality_bad",
        "distinct_qualitys",
        "max_reading_kwh",
        "reading_count",
        "reading_id",
        "reading_read_at",
    )
    .orderBy(F.col("reading_count").desc(), F.col("site_id").desc())
)
