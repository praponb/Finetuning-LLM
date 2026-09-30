from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_site_id = Window.partitionBy("reading_quality", "site_id")
w_reading_id_reading_quality_b = Window.partitionBy("reading_id", "reading_quality").orderBy("reading_kwh", "id", "reading_id")
w_site_id_reading_id_by_readin = Window.partitionBy("site_id", "reading_id").orderBy("reading_kwh", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter((F.col("voltage").between(221.8, 232.4)) & (F.col("quality").isin("BAD", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

line_level_readings = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

screened = (
    line_level_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")), "left_anti")
)

with_windows = (
    screened
    .select(
        "*",
        F.count(F.lit(1)).over(w_reading_quality_site_id).alias("rows_in_reading_quality_site_id"),
        F.lag("reading_kwh").over(w_reading_id_reading_quality_b).alias("prev_reading_kwh"),
        F.min("reading_kwh").over(w_site_id_reading_id_by_readin).alias("min_reading_kwh_so_far"),
        F.min("reading_kwh").over(w_reading_quality_site_id).alias("reading_quality_site_id_min_reading_kwh"),
        F.count(F.lit(1)).over(w_site_id_reading_id_by_readin).alias("running_count"),
        F.round(F.avg("reading_kwh").over(w_reading_quality_site_id), 2).alias("reading_quality_site_id_avg_reading_kwh"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["reading_id", "site_id"], ["reading_id"], ["site_id"], []],
        "reading_id", "site_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("running_count").alias("total_running_count"),
        F.round(F.sum("min_reading_kwh_so_far") / F.nullif(F.sum("prev_reading_kwh"), F.lit(0)), 3).alias("min_reading_kwh_so_far_to_prev_reading_kwh_ratio"),
        F.count("*").alias("cnt"),
    )
)
