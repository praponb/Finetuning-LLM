from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy("kwh", "id")

valid_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

unique_rows = (
    valid_readings
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_stats = (
    unique_rows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

enriched = (
    with_stats
    .filter(F.col("site_id") >= 8)
    .select(
        "*",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
    )
)

result = (
    enriched
    .cube("reading_count", "site_id")
    .agg(
        F.grouping("reading_count").alias("is_total_reading_count"),
        F.max("read_at_plus_7d").alias("last_read_at_plus_7d"),
    )
    .orderBy(
        F.col("reading_count").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
