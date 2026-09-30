from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_week_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at_week"), "site_id")
w_quality_by_kwh_full = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_read_at_full = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id")

base = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("voltage").alias("min_reading_voltage"),
        F.min("kwh").alias("min_reading_kwh"),
        F.sum("voltage").alias("total_reading_voltage"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.min_reading_voltage",
        "m.min_reading_kwh",
        "m.total_reading_voltage",
    )
)

screened = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

sequenced = (
    screened
    .select(
        "*",
        F.round(F.col("min_reading_voltage") / F.nullif(F.sum("min_reading_voltage").over(w_read_at_week_month_site_id), F.lit(0)), 4).alias("min_reading_voltage_share_of_read_at_week_month_"),
        F.size(F.collect_set("quality").over(w_read_at_week_month_site_id)).alias("distinct_quality_in_read_at_week_month_site_id"),
        F.nth_value("total_reading_voltage", 2).over(w_quality_by_kwh_full).alias("second_total_reading_voltage"),
        F.nth_value("total_reading_voltage", 2).over(w_quality_by_read_at_full).alias("second_total_reading_voltage_2"),
        (F.col("min_reading_voltage") - F.lag("min_reading_voltage").over(w_quality_by_kwh)).alias("min_reading_voltage_change"),
        F.count(F.lit(1)).over(w_read_at_week_month_site_id).alias("rows_in_read_at_week_month_site_id"),
    )
)

grouped = (
    sequenced
    .groupBy(
        F.to_date("read_at").alias("read_at_day"),
        F.when(F.col("min_reading_kwh") >= 72.0, "large").when(F.col("min_reading_kwh") >= 42.0, "medium").otherwise("small").alias("min_reading_kwh_tier"),
    )
    .agg(
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.sum(F.when(F.col("quality") == "OK", F.col("second_total_reading_voltage"))).alias("second_total_reading_voltage_quality_ok"),
        F.sum("voltage").alias("total_voltage"),
        F.count("*").alias("cnt"),
        F.min("rows_in_read_at_week_month_site_id").alias("min_rows_in_read_at_week_month_site_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "read_at_day",
        "min_reading_kwh_tier",
        "total_voltage",
        "min_rows_in_read_at_week_month_site_id",
        F.abs(F.col("cnt") - 8).alias("cnt_distance"),
    )
    .orderBy(F.col("min_reading_kwh_tier").desc(), "read_at_day")
)
