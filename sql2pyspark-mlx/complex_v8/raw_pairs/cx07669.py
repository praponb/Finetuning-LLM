from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_last_reading_read = Window.partitionBy("site_id").orderBy(F.col("last_reading_read_at").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_quality_by_kwh_running = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality = Window.partitionBy("quality")

prepared = (
    df_meter_readings
    .filter(F.col("site_id") <= 5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.min("kwh").alias("min_reading_kwh"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        "m.avg_reading_kwh",
        "m.min_reading_kwh",
    )
)

ranked = (
    with_stats
    .select(
        "id",
        "read_at",
        "kwh",
        "quality",
        "min_reading_kwh",
        F.max("min_reading_kwh").over(w_site_id_by_last_reading_read).alias("min_reading_kwh_max_3"),
        F.sum("kwh").over(w_quality_by_kwh_running).alias("running_kwh"),
        F.round(F.avg("kwh").over(w_quality), 2).alias("quality_avg_kwh"),
    )
)

scored = (
    ranked
    .select(
        "*",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

enriched = (
    scored
    .filter(F.col("running_kwh") < 720.0)
    .select(
        "id",
        "read_at",
        "quality",
        "min_reading_kwh",
        "min_reading_kwh_max_3",
        "running_kwh",
        "quality_avg_kwh",
        "quality_prefix",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix_2"),
    )
)

result = (
    enriched
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "read_at",
        "quality",
        "min_reading_kwh",
        "min_reading_kwh_max_3",
        "running_kwh",
        "quality_avg_kwh",
        "quality_prefix",
        "quality_prefix_2",
    )
    .orderBy("quality_prefix", F.col("quality_avg_kwh").desc_nulls_first())
)
