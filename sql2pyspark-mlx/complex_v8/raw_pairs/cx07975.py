from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_commissioned_on_full = Window.orderBy(F.col("commissioned_on").desc(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_read_at_month_readin = Window.partitionBy(F.month("reading_read_at"), "reading_count")
w_all_by_commissioned_on = Window.orderBy(F.col("commissioned_on").desc(), "id", "reading_id")

scoped_sites = (
    df_sites
    .filter((F.col("tariff") == "FLAT") & (F.upper(F.trim(F.col("site_name"))).like("D%")))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count(F.when(F.col("quality").isin("EST", "BAD"), 1)).alias("reading_quality_count"),
        F.max("voltage").alias("max_reading_voltage"),
    )
)

with_stats = (
    scoped_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.max_reading_voltage",
    )
)

detail_rows_readings = (
    with_stats.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.zone",
        "s.installed_kw",
        "s.commissioned_on",
        "s.reading_count",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

derived = (
    detail_rows_readings
    .select(
        "*",
        F.concat(F.col("zone"), F.lit("#"), F.col("reading_id").cast("string")).alias("zone_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

metrics = (
    derived
    .select(
        "*",
        F.nth_value("reading_kwh", 2).over(w_all_by_commissioned_on_full).alias("second_reading_kwh"),
        F.min("reading_kwh").over(w_reading_read_at_month_readin).alias("reading_read_at_month_reading_count_min_reading_"),
        F.nth_value("commissioned_on", 2).over(w_all_by_commissioned_on_full).alias("second_commissioned_on"),
        F.ntile(3).over(w_all_by_commissioned_on).alias("commissioned_on_tercile"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.sum("voltage").alias("total_reading_voltage"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

summarized_rows = (
    metrics.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "m.min_reading_voltage",
        "m.total_reading_voltage",
        "m.last_reading_read_at",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "zone",
        "reading_count",
        "reading_id",
        "reading_read_at",
        "zone_slug",
        "second_reading_kwh",
        "reading_read_at_month_reading_count_min_reading_",
        "second_commissioned_on",
        "min_reading_voltage",
        "total_reading_voltage",
        "last_reading_read_at",
    )
    .orderBy("second_commissioned_on")
)
