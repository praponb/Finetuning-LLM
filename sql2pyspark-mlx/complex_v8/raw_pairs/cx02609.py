from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("tariff").orderBy(F.col("commissioned_on").desc(), F.col("id").desc())
w_latest = Window.partitionBy("tariff").orderBy(F.col("installed_kw").desc(), F.col("id").desc())

recent_sites = df_sites.filter(F.col("zone").isin("Z3", "Z4"))

unique_rows = (
    recent_sites
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

latest_per_key = (
    unique_rows
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
        F.min("kwh").alias("min_reading_kwh"),
    )
)

profiled = (
    latest_per_key.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
        "m.min_reading_kwh",
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    profiled.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.avg_reading_kwh",
        "m.first_reading_read_at",
        "m.last_reading_read_at",
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left_semi")
)

reading_stats_3 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.sum("voltage").alias("total_reading_voltage"))
)

with_counts = (
    with_activity.alias("s")
    .join(reading_stats_3.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.total_reading_voltage")
)

result = (
    with_counts
    .groupBy("commissioned_on")
    .pivot("zone", ["Z4", "Z2", "Z3"])
    .agg(F.max("min_reading_kwh"))
)
