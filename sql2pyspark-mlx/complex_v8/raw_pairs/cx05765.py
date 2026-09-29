from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_reading_voltage = Window.partitionBy("site_id").orderBy("reading_voltage", "id", "reading_id")
w_kwh_tier = Window.partitionBy("kwh_tier")

valid = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) & (F.col("kwh").between(20.4, 84.9)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 104.8, "HIGH").when(F.col("kwh") >= 35.9, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
        F.abs(F.col("kwh") - 42.1).alias("kwh_distance"),
    )
)

qualified = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(218.5, 240.9)), "left_semi")
    .filter(F.col("voltage") >= 219.6)
)

with_children = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.kwh_tier",
        "s.kwh_distance",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

with_stats = (
    with_children.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

screened = (
    with_stats
    .crossJoin(with_stats.agg(F.avg("reading_voltage").alias("overall_avg_reading_voltage")))
    .filter(F.col("reading_voltage") > F.col("overall_avg_reading_voltage"))
    .drop("overall_avg_reading_voltage")
)

metrics = (
    screened
    .select(
        "id",
        "kwh",
        "quality",
        "kwh_distance",
        "reading_id",
        "reading_read_at",
        "reading_kwh",
        "reading_quality",
        "reading_voltage",
        "first_reading_read_at",
        F.lag("kwh").over(w_site_id_by_reading_voltage).alias("prev_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_kwh_tier), 2).alias("kwh_vs_kwh_tier_avg"),
    )
)

result = (
    metrics
    .rollup("reading_quality", "reading_id", "quality")
    .agg(
        F.min("kwh_vs_kwh_tier_avg").alias("min_kwh_vs_kwh_tier_avg"),
        F.max("reading_kwh").alias("max_reading_kwh"),
        F.sum(F.when(F.col("reading_quality").isin("BAD", "OK"), F.col("reading_voltage"))).alias("reading_voltage_reading_quality_in_bad_ok"),
    )
    .select(
        F.coalesce(F.col("reading_quality"), F.lit("ALL")).alias("reading_quality"),
        "reading_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "min_kwh_vs_kwh_tier_avg",
        "max_reading_kwh",
        "reading_voltage_reading_quality_in_bad_ok",
    )
)
