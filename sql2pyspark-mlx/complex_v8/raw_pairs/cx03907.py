from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_quality_by_read = Window.partitionBy(F.year("read_at"), "quality").orderBy("read_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_quality_by_read_2 = Window.partitionBy(F.year("read_at"), "quality").orderBy("read_at_year", "id").rowsBetween(-3, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_site_zone_quality_by_read_at = Window.partitionBy("site_zone", "quality").orderBy("read_at", "id")
w_site_zone_quality_by_read_at_2 = Window.partitionBy("site_zone", "quality").orderBy("read_at", "id").rowsBetween(-3, Window.currentRow)
w_read_at_year_month = Window.partitionBy(F.month("read_at_year"))
w_read_at_year_quality_by_read_3 = Window.partitionBy(F.year("read_at"), "quality").orderBy("read_at_year", "id")
w_site_id_read_at_year_tercile = Window.partitionBy("site_id", "read_at_year_tercile").orderBy(F.col("read_at_year").asc_nulls_last())
w_all_by_voltage = Window.orderBy(F.col("voltage").asc_nulls_last(), "id")
w_site_zone_by_kwh_avg_4_movin = Window.partitionBy("site_zone").orderBy("kwh_avg_4", "id").rowsBetween(-2, Window.currentRow)
w_site_id_read_at_year_tercile_2 = Window.partitionBy("site_id", "read_at_year_tercile").orderBy(F.col("read_at_year").asc_nulls_last(), "id")
w_all_by_voltage_moving_3 = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh_avg_4_ties = Window.orderBy(F.col("kwh_avg_4").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("voltage") < 233.6) & (F.hour(F.col("read_at")).between(10, 14)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

with_parent = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_year",
        F.col("si.installed_kw").alias("site_installed_kw"),
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_year",
        "site_installed_kw",
        "site_zone",
        F.nth_value("read_at_year", 2).over(w_read_at_year_quality_by_read).alias("second_read_at_year"),
        F.round(F.avg("site_installed_kw").over(w_read_at_year_quality_by_read_2), 2).alias("site_installed_kw_avg_4"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_id), 2).alias("site_installed_kw_vs_site_id_avg"),
        F.lead("read_at", 3).over(w_site_zone_quality_by_read_at).alias("next3_read_at"),
        F.round(F.avg("kwh").over(w_site_zone_quality_by_read_at_2), 2).alias("kwh_avg_4"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id), F.lit(0)), 4).alias("kwh_share_of_site_id"),
        F.size(F.collect_set("site_id").over(w_read_at_year_month)).alias("distinct_site_id_in_read_at_year_month"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_year_month), 2).alias("voltage_vs_read_at_year_month_avg"),
        F.ntile(3).over(w_read_at_year_quality_by_read_3).alias("read_at_year_tercile"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w_site_id_read_at_year_tercile).alias("read_at_year_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_voltage), 3).alias("voltage_percent_rank"),
        F.max("voltage").over(w_site_zone_by_kwh_avg_4_movin).alias("voltage_max_3"),
        F.round((F.unix_timestamp("read_at_year") - F.unix_timestamp(F.lag("read_at_year").over(w_site_id_read_at_year_tercile_2))) / 60, 1).alias("minutes_since_prev_read_at_year"),
        F.round(F.avg("site_installed_kw").over(w_all_by_voltage_moving_3), 2).alias("site_installed_kw_avg_4_2"),
        F.dense_rank().over(w_all_by_kwh_avg_4_ties).alias("kwh_avg_4_dense_rank"),
    )
)

result = (
    metrics_2
    .filter(F.col("read_at_year") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_year",
        "site_installed_kw",
        "site_installed_kw_avg_4",
        "site_installed_kw_vs_site_id_avg",
        "distinct_site_id_in_read_at_year_month",
        "voltage_vs_read_at_year_month_avg",
        "read_at_year_tercile",
        "voltage_percent_rank",
        "voltage_max_3",
        "minutes_since_prev_read_at_year",
        "site_installed_kw_avg_4_2",
        "kwh_avg_4_dense_rank",
        F.substring(F.trim(F.col("site_zone")), 1, 2).alias("site_zone_prefix"),
        F.dayofweek(F.col("read_at_year")).alias("read_at_year_dayofweek"),
    )
    .orderBy("quality")
)
