from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("kwh") < 101.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
        F.count(F.when(F.col("quality") != "BAD", 1)).alias("reading_quality_count"),
    )
)

profiled = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_voltage",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["site_id", "quality", "reading_quality_count"], ["site_id"], ["quality"], []],
        "site_id", "quality", "reading_quality_count",
    )
    .agg(
        F.round(F.sum("voltage"), 2).alias("total_voltage"),
        F.sum("reading_quality_count").alias("total_reading_quality_count"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "reading_quality_count",
        "total_voltage",
        "total_reading_quality_count",
    )
)
