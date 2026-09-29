from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("reading_count_2").desc(), F.col("id").desc())

valid = (
    df_meter_readings
    .filter((F.col("kwh") > 45.0) & (F.col("voltage") >= 218.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

screened = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
    .filter(F.col("quality_len") <= 12)
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("voltage").alias("min_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_counts = (
    screened.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.min_reading_voltage",
        "m.last_reading_read_at",
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count_2"),
        F.max("read_at").alias("last_reading_read_at_2"),
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
    )
)

summarized_rows = (
    with_counts.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
        "m.last_reading_read_at_2",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

reading_stats_3 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_reading_voltage"))
)

profiled = (
    summarized_rows.alias("s")
    .join(reading_stats_3.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.max_reading_voltage")
)

unique_rows = (
    profiled
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_hour",
        "min_reading_voltage",
        "last_reading_read_at",
        "reading_count_2",
        "reading_quality_count",
        "max_reading_voltage",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.round(F.col("max_reading_voltage") * 1.1, 2).alias("max_reading_voltage_adj"),
    )
    .orderBy("read_at", "min_reading_voltage")
)
