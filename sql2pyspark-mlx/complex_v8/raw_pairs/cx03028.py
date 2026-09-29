from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_voltage_full = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_per_voltage_runni = Window.orderBy(F.col("kwh_per_voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_per_voltage = Window.orderBy(F.col("kwh_per_voltage").asc_nulls_last(), "id")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh_per_voltage_movin = Window.orderBy(F.col("kwh_per_voltage").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_second_read_at_to_date_by_la = Window.partitionBy(F.to_date("second_read_at")).orderBy(F.col("last_known_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_read_at_to_date_by_la_2 = Window.partitionBy(F.to_date("second_read_at")).orderBy(F.col("last_known_read_at").desc(), "id")
w_second_read_at_month_quality = Window.partitionBy(F.month("second_read_at"), "quality")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_quality_by_last_known_read_a = Window.partitionBy("quality").orderBy(F.col("last_known_read_at").desc(), "id")

valid = (
    df_meter_readings
    .filter((F.col("kwh") >= 52.4) & (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

filtered = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 87.1), "left_anti")
)

metrics = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.nth_value("read_at", 2).over(w_all_by_voltage_full).alias("second_read_at"),
        F.last("read_at", ignorenulls=True).over(w_all_by_kwh_per_voltage_runni).alias("last_known_read_at"),
        F.lag("voltage").over(w_all_by_kwh_per_voltage).alias("prev_voltage"),
        F.sum("kwh").over(w_all_by_voltage_running).alias("running_kwh"),
        F.sum("voltage").over(w_all_by_kwh_per_voltage_movin).alias("voltage_sum_7"),
    )
)

site_id_stats = (
    metrics
    .groupBy("site_id")
    .agg(F.avg("running_kwh").alias("avg_running_kwh"))
)

compared = (
    metrics.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.running_kwh") > F.col("g.avg_running_kwh"))
    .select(
        "s.*",
        "g.avg_running_kwh",
        F.round(F.col("s.running_kwh") - F.col("g.avg_running_kwh"), 2).alias("running_kwh_minus_avg"),
    )
)

windowed = (
    compared
    .select(
        "id",
        "site_id",
        "quality",
        "kwh_share_of_site_id",
        "last_known_read_at",
        "prev_voltage",
        "running_kwh",
        "voltage_sum_7",
        "running_kwh_minus_avg",
        F.sum("kwh_share_of_site_id").over(w_second_read_at_to_date_by_la).alias("running_kwh_share_of_site_id"),
        F.lag("avg_running_kwh").over(w_second_read_at_to_date_by_la_2).alias("prev_avg_running_kwh"),
        F.min("voltage").over(w_second_read_at_month_quality).alias("second_read_at_month_quality_min_voltage"),
        (F.col("voltage_sum_7") - F.lag("voltage_sum_7").over(w_site_id_by_read_at)).alias("voltage_sum_7_change"),
        F.lead("voltage", 2, 0).over(w_quality_by_last_known_read_a).alias("next2_voltage"),
    )
)

joined_site = (
    windowed.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.upper(F.trim(F.col("si.site_name"))).like("M%"))
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.kwh_share_of_site_id",
        "s.last_known_read_at",
        "s.voltage_sum_7",
        "s.running_kwh_share_of_site_id",
        "s.prev_avg_running_kwh",
        "s.second_read_at_month_quality_min_voltage",
        "s.voltage_sum_7_change",
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.min("read_at").alias("first_reading_read_at_2"),
    )
)

summarized_rows = (
    joined_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.first_reading_read_at_2",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "quality",
        "last_known_read_at",
        "voltage_sum_7",
        "running_kwh_share_of_site_id",
        "prev_avg_running_kwh",
        "second_read_at_month_quality_min_voltage",
        "voltage_sum_7_change",
        "site_name",
        "first_reading_read_at",
        "reading_quality_count",
        "first_reading_read_at_2",
    )
    .orderBy(F.col("first_reading_read_at").desc(), "id")
    .limit(10)
)
